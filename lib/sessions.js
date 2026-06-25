'use strict';

const state = require('./state');
const { newSessionId } = require('./coalesce');
// Defensive: groupSummaryTimers was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!(state.groupSummaryTimers instanceof Map)) state.groupSummaryTimers = new Map();

// Per-model learned char→token ratio, accumulated from completed requests
// that returned exact usage. Used to estimate tok/s DURING streaming
// (before the final usage chunk arrives). The rolling TPS bucket is
// NEVER fed from this estimate — only from the final exact count at _finish.
function estimateTokensFromChars(model, chars) {
  const r = state.modelCharRatio.get(model);
  if (r && r.chars > 0) return Math.round(chars * (r.tokens / r.chars));
  return Math.floor(chars / 4);
}

function updateModelRatio(model, chars, tokens) {
  if (!model || chars <= 0 || tokens <= 0) return;
  const r = state.modelCharRatio.get(model) || { chars: 0, tokens: 0 };
  r.chars += chars;
  r.tokens += tokens;
  state.modelCharRatio.set(model, r);
}

function createSession({ model, stream, groupKey }) {
  const id = newSessionId();
  if (model) state.seenModels.add(model);
  const session = {
    id, model, stream: !!stream,
    groupKey: groupKey || id,
    startedAt: Date.now(),
    status: 'active',
    outputTokens: 0,
    exactTokens: false,
    chars: 0,
    bytes: 0,
    promptTokens: 0,
    cachedTokens: 0,
    completionTokens: 0,
    responseContent: '',
    finalTps: null,
    firstTokenAt: null,
  };
  state.sessions.set(id, session);
  return session;
}

function updateGroupSummary(session) {
  if (!session.groupKey) return;
  const g = state.groupSummaries.get(session.groupKey) || {
    groupKey: session.groupKey,
    model: session.model,
    turnCount: 0,
    totalTokens: 0,
    totalCached: 0,
    totalUncached: 0,
    totalBytes: 0,
    finalTpsSum: 0,
    finalTpsCount: 0,
    lastSeenAt: 0,
  };
  g.turnCount++;
  g.totalTokens += session.completionTokens || 0;
  g.totalCached += session.cachedTokens || 0;
  g.totalUncached += Math.max(0, (session.promptTokens || 0) - (session.cachedTokens || 0));
  g.totalBytes += session.bytes || 0;
  if (session.finalTps != null && session.finalTps > 0) {
    g.finalTpsSum = (g.finalTpsSum || 0) + session.finalTps;
    g.finalTpsCount = (g.finalTpsCount || 0) + 1;
  }
  g.lastSeenAt = Date.now();
  state.groupSummaries.set(session.groupKey, g);
  const prevTimer = state.groupSummaryTimers.get(session.groupKey);
  if (prevTimer) clearTimeout(prevTimer);
  const timer = setTimeout(() => {
    const entry = state.groupSummaries.get(session.groupKey);
    if (entry && Date.now() - entry.lastSeenAt >= state.GROUP_SUMMARY_TTL_MS) {
      state.groupSummaries.delete(session.groupKey);
    }
    state.groupSummaryTimers.delete(session.groupKey);
  }, state.GROUP_SUMMARY_TTL_MS);
  timer.unref();
  state.groupSummaryTimers.set(session.groupKey, timer);
}

function finalizeSession(session, status) {
  if (!session) return;
  session.status = status;
  session.endedAt = Date.now();
  if (session.outputTokens > 0 && session.firstTokenAt) {
    const genMs = Math.max(1, session.endedAt - session.firstTokenAt);
    session.finalTps = Math.round((session.outputTokens / genMs) * 1000 * 10) / 10;
  } else if (session.outputTokens > 0) {
    const elapsed = Math.max(1, session.endedAt - session.startedAt);
    session.finalTps = Math.round((session.outputTokens / elapsed) * 1000 * 10) / 10;
  }
  updateGroupSummary(session);
  // KV cache eviction signal: if this request got 0 cached tokens, the prefix
  // was not in upstream KV cache. Old sessions from the same group are stale
  // — the conversation's cache entry was evicted. Clean them up immediately
  // instead of waiting for the 120s timer. The current session stays (it just
  // completed) and expires via the normal timer below.
  if ((session.cachedTokens || 0) === 0 && session.promptTokens > 0) {
    for (const s of state.sessions.values()) {
      if (s.groupKey === session.groupKey && s.id !== session.id && s.status !== 'active') {
        state.sessions.delete(s.id);
      }
    }
  }
  setTimeout(() => { state.sessions.delete(session.id); scheduleSessionsBroadcast(); }, state.SESSION_TTL_MS).unref();
  scheduleSessionsBroadcast();
}

function addSessionTokens(session, tokens, { exact = false } = {}) {
  if (!session) return;
  if (exact) session.exactTokens = true;
  if (tokens <= 0) return;
  const now = Date.now();
  const cutoff = now - state.TPS_WINDOW_MS;
  state.tpsBuckets.push({ time: now, tokens });
  let i = 0;
  while (i < state.tpsBuckets.length && state.tpsBuckets[i].time < cutoff) i++;
  if (i > 0) state.tpsBuckets.splice(0, i);
  const model = session.model;
  if (model) {
    state.seenModels.add(model);
    let buckets = state.tpsBucketsByModel.get(model);
    if (!buckets) { buckets = []; state.tpsBucketsByModel.set(model, buckets); }
    buckets.push({ time: now, tokens });
    let j = 0;
    while (j < buckets.length && buckets[j].time < cutoff) j++;
    if (j > 0) buckets.splice(0, j);
  }
}

function aggregateTps() {
  const now = Date.now();
  const cutoff = now - state.TPS_WINDOW_MS;
  let tokens = 0;
  for (const b of state.tpsBuckets) if (b.time >= cutoff) tokens += b.tokens;
  return tokens / (state.TPS_WINDOW_MS / 1000);
}

function modelTpsBreakdown() {
  const now = Date.now();
  const cutoff = now - state.TPS_WINDOW_MS;
  const out = [];
  for (const model of state.seenModels) {
    const buckets = state.tpsBucketsByModel.get(model);
    if (!buckets) { out.push({ model, tps: 0 }); continue; }
    let tokens = 0;
    for (const b of buckets) if (b.time >= cutoff) tokens += b.tokens;
    out.push({ model, tps: tokens / (state.TPS_WINDOW_MS / 1000) });
  }
  out.sort((a, b) => b.tps - a.tps);
  return out;
}

function sessionTps(session) {
  if (!session) return 0;
  const now = Date.now();
  const end = session.endedAt ?? now;
  const elapsed = Math.max(1, end - session.startedAt);
  const window = Math.min(elapsed, state.TPS_WINDOW_MS);
  return (session.outputTokens / window) * 1000;
}

function sessionPublicView(session) {
  const now = Date.now();
  const endedAt = session.endedAt;
  const elapsed = (endedAt ?? now) - session.startedAt;
  const active = session.status === 'active';
  return {
    id: session.id,
    groupKey: session.groupKey,
    model: session.model,
    stream: session.stream,
    status: session.status,
    active,
    startedAt: new Date(session.startedAt).toISOString(),
    endedAt: endedAt ? new Date(endedAt).toISOString() : null,
    elapsedMs: elapsed,
    outputTokens: session.outputTokens,
    exactTokens: session.exactTokens,
    promptTokens: session.promptTokens,
    cachedTokens: session.cachedTokens,
    uncachedTokens: Math.max(0, session.promptTokens - session.cachedTokens),
    completionTokens: session.completionTokens,
    cacheHitRate: session.promptTokens > 0 ? Math.round((session.cachedTokens / session.promptTokens) * 1000) / 10 : null,
    bytes: session.bytes,
    tps: Math.round((!active && session.finalTps != null ? session.finalTps : sessionTps(session)) * 10) / 10,
    finalTps: session.finalTps,
  };
}

function getSessionsSnapshot() {
  const list = [...state.sessions.values()].map(sessionPublicView);
  list.sort((a, b) => (a.active === b.active ? b.startedAt.localeCompare(a.startedAt) : a.active ? -1 : 1));
  const tpsByModel = new Map();
  const tpsValues = [];
  for (const s of list) {
    if (!s.active && s.finalTps != null && s.finalTps > 0) {
      tpsValues.push(s.finalTps);
      let arr = tpsByModel.get(s.model);
      if (!arr) { arr = []; tpsByModel.set(s.model, arr); }
      arr.push(s.finalTps);
    }
  }
  tpsValues.sort((a, b) => a - b);
  for (const arr of tpsByModel.values()) arr.sort((a, b) => a - b);
  const percentile = (arr, p) => {
    if (!arr.length) return null;
    const idx = Math.min(arr.length - 1, Math.max(0, Math.floor((arr.length - 1) * p)));
    return Math.round(arr[idx] * 10) / 10;
  };
  const medianTps = percentile(tpsValues, 0.5);
  const p10Tps = percentile(tpsValues, 0.1);
  const liveModelTps = modelTpsBreakdown();
  const models = liveModelTps.map((m) => {
    const vals = tpsByModel.get(m.model);
    return {
      model: m.model,
      tps: Math.round(m.tps * 10) / 10,
      medianTps: vals ? percentile(vals, 0.5) : null,
      p10Tps: vals ? percentile(vals, 0.1) : null,
    };
  });
  const groups = [...state.groupSummaries.values()].map((g) => {
    const avgFinal = g.finalTpsCount
      ? Math.round(g.finalTpsSum / g.finalTpsCount * 10) / 10
      : null;
    return {
      groupKey: g.groupKey, model: g.model, turnCount: g.turnCount,
      totalTokens: g.totalTokens, totalCached: g.totalCached,
      totalUncached: g.totalUncached, totalBytes: g.totalBytes,
      avgFinalTps: avgFinal, lastSeenAt: g.lastSeenAt,
    };
  });
  return {
    sessions: list,
    groupSummaries: groups,
    aggregate: {
      tps: Math.round(aggregateTps() * 10) / 10,
      activeSessions: (() => { let n = 0; for (const s of state.sessions.values()) if (s.status === 'active') n++; return n; })(),
      totalSessions: list.length,
      medianTps,
      p10Tps,
      models,
    },
  };
}

function scheduleSessionsBroadcast() {
  if (state.broadcastThrottled) return;
  let hasActive = false;
  for (const s of state.sessions.values()) { if (s.status === 'active') { hasActive = true; break; } }
  if (!hasActive) {
    if (state.sessionsBroadcastTimer) { clearInterval(state.sessionsBroadcastTimer); state.sessionsBroadcastTimer = null; }
    broadcastEvent('sessions', getSessionsSnapshot());
    return;
  }
  state.broadcastThrottled = true;
  setTimeout(() => {
    state.broadcastThrottled = false;
    broadcastEvent('sessions', getSessionsSnapshot());
    if (!state.sessionsBroadcastTimer) {
      state.sessionsBroadcastTimer = setInterval(() => {
        let stillActive = false;
        for (const s of state.sessions.values()) { if (s.status === 'active') { stillActive = true; break; } }
        broadcastEvent('sessions', getSessionsSnapshot());
        if (!stillActive && state.sessionsBroadcastTimer) {
          clearInterval(state.sessionsBroadcastTimer);
          state.sessionsBroadcastTimer = null;
        }
      }, state.SESSION_BROADCAST_MIN_MS);
    }
  }, 0);
}

const { send: wsSend } = require('./ws');

function broadcastEvent(event, data) {
  if (state.wsClients.size === 0) return;
  const payload = JSON.stringify({ type: event, data });
  for (const client of state.wsClients) {
    if (client.alive) wsSend(client, payload);
    else state.wsClients.delete(client);
  }
}

module.exports = {
  estimateTokensFromChars,
  updateModelRatio,
  createSession,
  updateGroupSummary,
  finalizeSession,
  addSessionTokens,
  aggregateTps,
  modelTpsBreakdown,
  sessionTps,
  sessionPublicView,
  getSessionsSnapshot,
  scheduleSessionsBroadcast,
  broadcastEvent,
};
