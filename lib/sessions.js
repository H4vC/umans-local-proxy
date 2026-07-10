'use strict';

const state = require('./state');
const { newSessionId } = require('./coalesce');
const { SCALING_DEFAULTS } = require('./config');
// Defensive: groupSummaryTimers was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!(state.groupSummaryTimers instanceof Map)) state.groupSummaryTimers = new Map();
if (!(state.sessionsByGroup instanceof Map)) state.sessionsByGroup = new Map();
if (!(state.sessionTimers instanceof Map)) state.sessionTimers = new Map();
if (!Array.isArray(state.completedSessionOrder)) state.completedSessionOrder = [];
if (!Number.isInteger(state.completedSessionHead) || state.completedSessionHead < 0) state.completedSessionHead = 0;
// Rebuild once on module load so hot reloads inherit a coherent completion
// order even when the previous module version had no eviction index.
state.completedSessionOrder = [...state.sessions.values()]
  .filter((session) => session.status === 'done')
  .sort((a, b) => (a.endedAt || 0) - (b.endedAt || 0))
  .map((session) => session.id);
state.completedSessionHead = 0;

const DEFAULT_SESSION_HISTORY_LIMIT = SCALING_DEFAULTS.sessionHistory;
const DEFAULT_GROUP_SUMMARY_LIMIT = SCALING_DEFAULTS.groupSummaries;
const DEFAULT_SEEN_MODELS_LIMIT = SCALING_DEFAULTS.seenModels;
const DEFAULT_MODEL_RATIO_LIMIT = SCALING_DEFAULTS.modelRatios;

function configuredLimit(name, fallback) {
  const value = Number(state.config?.limits?.[name]);
  return Number.isSafeInteger(value) && value >= 0 ? value : fallback;
}

function sessionHistoryLimit() { return configuredLimit('sessionHistory', DEFAULT_SESSION_HISTORY_LIMIT); }
function groupSummaryLimit() { return configuredLimit('groupSummaries', DEFAULT_GROUP_SUMMARY_LIMIT); }
function seenModelsLimit() { return configuredLimit('seenModels', DEFAULT_SEEN_MODELS_LIMIT); }
function modelRatioLimit() { return configuredLimit('modelRatios', DEFAULT_MODEL_RATIO_LIMIT); }

// Per-model learned char→token ratio, accumulated from completed requests
// that returned exact usage. Used to estimate each in-flight session's live
// tok/s during streaming (before the final usage chunk arrives). Finished
// turns still drive the historical median/p10 columns and finalTps.
function estimateTokensFromChars(model, chars) {
  const r = state.modelCharRatio.get(model);
  if (r && r.chars > 0) return Math.round(chars * (r.tokens / r.chars));
  return Math.floor(chars / 4);
}

function updateModelRatio(model, chars, tokens) {
  if (!model || chars <= 0 || tokens <= 0) return;
  let r = state.modelCharRatio.get(model);
  if (!r) {
    if (state.modelCharRatio.size >= modelRatioLimit()) return;
    r = { chars: 0, tokens: 0 };
  }
  r.chars += chars;
  r.tokens += tokens;
  state.modelCharRatio.set(model, r);
}

function rememberModel(model) {
  if (!model || state.seenModels.has(model) || state.seenModels.size >= seenModelsLimit()) return;
  state.seenModels.add(model);
}

// Active sessions are always retained for correctness. Only successfully
// completed sessions consume the bounded history budget; aborted/error records
// expire through their normal TTL instead of evicting live or successful work.
function compactCompletedSessionOrder() {
  if (state.completedSessionHead < 1024 || state.completedSessionHead * 2 < state.completedSessionOrder.length) return;
  state.completedSessionOrder = state.completedSessionOrder.slice(state.completedSessionHead);
  state.completedSessionHead = 0;
}

function recordCompletedSession(session) {
  if (session?.status !== 'done') return;
  state.completedSessionOrder.push(session.id);
}

function oldestCompletedSession() {
  while (state.completedSessionHead < state.completedSessionOrder.length) {
    const id = state.completedSessionOrder[state.completedSessionHead++];
    const session = state.sessions.get(id);
    if (session?.status === 'done') return id;
  }
  compactCompletedSessionOrder();
  // Compatibility with records created by older code or tests that set the
  // terminal status directly without recording completion order.
  for (const [id, session] of state.sessions) {
    if (session.status === 'done') return id;
  }
  return null;
}

function enforceSessionHistoryLimit() {
  let completed = 0;
  for (const session of state.sessions.values()) {
    if (session.status === 'done') completed++;
  }
  const limit = sessionHistoryLimit();
  while (completed > limit) {
    const victim = oldestCompletedSession();
    if (!victim) break;
    deleteSession(victim);
    completed--;
  }
  compactCompletedSessionOrder();
}

function dropGroupSummary(groupKey) {
  clearTimeout(state.groupSummaryTimers.get(groupKey));
  state.groupSummaryTimers.delete(groupKey);
  state.groupSummaries.delete(groupKey);
}

function reserveGroupSummarySlot() {
  if (state.groupSummaries.size < groupSummaryLimit()) return true;
  let victim;
  let oldest = Infinity;
  for (const [key, summary] of state.groupSummaries) {
    const seen = summary.lastSeenAt || 0;
    if (seen < oldest) { oldest = seen; victim = key; }
  }
  if (!victim) return false;
  dropGroupSummary(victim);
  return true;
}

function createSession({ model, stream, groupKey }) {
  const id = newSessionId();
  rememberModel(model);
  const startedAt = Date.now();
  const session = {
    id, model, stream: !!stream,
    groupKey: groupKey || id,
    startedAt,
    startedAtIso: new Date(startedAt).toISOString(),
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
  // Index sessions by group so cache-miss eviction is O(group size), not O(all
  // sessions). Kept in sync by deleteSession; clear-state clears the whole map.
  let grp = state.sessionsByGroup.get(session.groupKey);
  if (!grp) { grp = new Set(); state.sessionsByGroup.set(session.groupKey, grp); }
  grp.add(id);
  enforceSessionHistoryLimit();
  return session;
}

// Remove a session from state.sessions and the group index together. Callers
// MUST use this instead of state.sessions.delete or the index desynchronizes.
function deleteSession(id) {
  clearTimeout(state.sessionTimers.get(id));
  state.sessionTimers.delete(id);
  const s = state.sessions.get(id);
  if (s && s.groupKey) {
    const set = state.sessionsByGroup.get(s.groupKey);
    if (set) { set.delete(id); if (set.size === 0) state.sessionsByGroup.delete(s.groupKey); }
  }
  state.sessions.delete(id);
}

function updateGroupSummary(session) {
  if (!session.groupKey) return;
  let g = state.groupSummaries.get(session.groupKey);
  if (!g) {
    if (!reserveGroupSummarySlot()) return;
    g = {
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
  }
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
  clearTimeout(state.groupSummaryTimers.get(session.groupKey));
  const timer = setTimeout(() => {
    // clearTimeout on refresh guarantees only the latest timer fires, so the
    // entry is stale. (The prior lastSeenAt guard could be false on early fire,
    // leaking the entry with no timer — always delete; premature deletes recover.)
    if (state.groupSummaries.has(session.groupKey)) state.groupSummaries.delete(session.groupKey);
    state.groupSummaryTimers.delete(session.groupKey);
  }, state.GROUP_SUMMARY_TTL_MS);
  timer.unref();
  state.groupSummaryTimers.set(session.groupKey, timer);
}

function finalizeSession(session, status) {
  if (!session) return;
  if (session.endedAt) return; // double-finalize would double-count the group summary + leak a TTL timer
  session.status = status;
  session.endedAt = Date.now();
  if (session.outputTokens > 0 && session.firstTokenAt) {
    const genMs = Math.max(1, session.endedAt - session.firstTokenAt);
    session.finalTps = Math.round((session.outputTokens / genMs) * 1000 * 10) / 10;
  } else if (session.outputTokens > 0) {
    const elapsed = Math.max(1, session.endedAt - session.startedAt);
    session.finalTps = Math.round((session.outputTokens / elapsed) * 1000 * 10) / 10;
  }
  session.endedAtIso = new Date(session.endedAt).toISOString();
  // Live TPS samples are no longer needed once the session is finalized —
  // finished sessions report measured finalTps and rateFor() never calls
  // sessionTps() on them. Drop the array so it isn't pinned for the 5-min TTL.
  session.tpsSamples = null;
  session.tpsHead = 0;
  // An evicted active session may still finish in chat.js. Keep its local
  // final fields coherent, but do not recreate bounded dashboard/group state.
  if (state.sessions.get(session.id) !== session) return;
  if (status === 'done') {
    updateGroupSummary(session);
    recordCompletedSession(session);
  }
  // KV cache eviction signal: if this request got 0 cached tokens, the prefix
  // was not in upstream KV cache. Old sessions from the same group are stale
  // — the conversation's cache entry was evicted. Clean them up immediately
  // instead of waiting for the 120s timer. The current session stays (it just
  // completed) and expires via the normal timer below.
  if (status === 'done' && (session.cachedTokens || 0) === 0 && session.promptTokens > 0) {
    // KV-cache miss: old sessions from this group are stale. Iterate the
    // group index (O(group size), not O(all sessions)) and drop finished ones.
    const grp = state.sessionsByGroup.get(session.groupKey);
    if (grp) {
      for (const sid of [...grp]) {
        if (sid === session.id) continue;
        const s = state.sessions.get(sid);
        if (s && s.status === 'done') deleteSession(sid);
      }
    }
  }
  enforceSessionHistoryLimit();
  if (state.sessions.get(session.id) !== session) return;
  const timer = setTimeout(() => {
    state.sessionTimers.delete(session.id);
    deleteSession(session.id);
    scheduleSessionsBroadcast();
  }, state.SESSION_TTL_MS);
  timer.unref();
  state.sessionTimers.set(session.id, timer);
  scheduleSessionsBroadcast();
}

function sessionTps(session) {
  if (!session) return 0;
  // Rolling TPS_WINDOW_MS window of cumulative token counts, fed by the tap
  // worker's ~100ms syncs (chat-tap.js). Rate = tokens produced in the window
  // — a true current rate, not cumulative/elapsed, so a long steady stream no
  // longer inflates without bound.
  const samples = session.tpsSamples;
  if (samples && samples.length >= 2) {
    const last = samples[samples.length - 1];
    const cutoff = (session.endedAt ?? Date.now()) - state.TPS_WINDOW_MS;
    let i = samples.length - 1;
    while (i > 0 && samples[i - 1].ts >= cutoff) i--;
    const first = samples[i];
    const span = last.ts - first.ts;
    if (span > 0) return ((last.tok - first.tok) / span) * 1000;
  }
  // Fallback: too few samples (session <~100ms old, or non-streaming). Use the
  // session-average over real elapsed time — never the old min(elapsed, window)
  // cap, which inflated any session older than the window.
  const end = session.endedAt ?? Date.now();
  const elapsed = Math.max(1, end - session.startedAt);
  return (session.outputTokens / elapsed) * 1000;
}

function sessionPublicView(session, tps) {
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
    startedAt: session.startedAtIso || new Date(session.startedAt).toISOString(),
    endedAt: endedAt ? (session.endedAtIso || new Date(endedAt).toISOString()) : null,
    elapsedMs: elapsed,
    outputTokens: session.outputTokens,
    exactTokens: session.exactTokens,
    promptTokens: session.promptTokens,
    cachedTokens: session.cachedTokens,
    uncachedTokens: Math.max(0, session.promptTokens - session.cachedTokens),
    completionTokens: session.completionTokens,
    cacheHitRate: session.promptTokens > 0 ? Math.round((session.cachedTokens / session.promptTokens) * 1000) / 10 : null,
    bytes: session.bytes,
    tps: Math.round((tps ?? 0) * 10) / 10,
    finalTps: session.finalTps,
    ttftMs: session.firstTokenAt != null ? Math.max(0, session.firstTokenAt - session.startedAt) : null,
  };
}

function getSessionsSnapshot() {
  const all = [...state.sessions.values()];

  // Single pass over FINISHED sessions for both the historical tps columns
  // (medianTps/p10, per model) and the proxy-derived TTFT p50. Previously this
  // was split across getSessionsSnapshot plus a second full walk in
  // sessionMetrics() that rebuilt an identical tpsByModel and re-sorted a copy
  // of every array per percentile call.
  const tpsByModel = new Map();
  const ttftByModel = new Map();
  const tpsValues = [];
  const ttftValues = [];
  for (const s of all) {
    if (s.status === 'active') continue;
    if (s.finalTps != null && s.finalTps > 0) {
      tpsValues.push(s.finalTps);
      let arr = tpsByModel.get(s.model);
      if (!arr) { arr = []; tpsByModel.set(s.model, arr); }
      arr.push(s.finalTps);
    }
    if (s.endedAt != null && s.firstTokenAt != null && s.startedAt != null) {
      const ttft = s.firstTokenAt - s.startedAt;
      if (ttft >= 0) {
        ttftValues.push(ttft);
        let arr = ttftByModel.get(s.model);
        if (!arr) { arr = []; ttftByModel.set(s.model, arr); }
        arr.push(ttft);
      }
    }
  }
  tpsValues.sort((a, b) => a - b);
  ttftValues.sort((a, b) => a - b);
  for (const arr of tpsByModel.values()) arr.sort((a, b) => a - b);
  for (const arr of ttftByModel.values()) arr.sort((a, b) => a - b);

  const percentile = (arr, p) => {
    if (!arr || !arr.length) return null;
    const idx = Math.min(arr.length - 1, Math.max(0, Math.floor((arr.length - 1) * p)));
    return Math.round(arr[idx] * 10) / 10;
  };
  const globalMedian = percentile(tpsValues, 0.5);

  const rateFor = (session) => session.status === 'active'
    ? sessionTps(session)
    : (session.finalTps ?? 0);

  // Sort raw sessions once by (active desc, startedAt desc) using numeric epoch
  // ms — avoids the O(n log n) localeCompare on ISO strings and the per-session
  // new Date() allocations the old view-level sort paid. Stable sort preserves
  // insertion order for equal (active, startedAt) pairs.
  all.sort((a, b) => {
    const aa = a.status === 'active' ? 1 : 0;
    const ba = b.status === 'active' ? 1 : 0;
    if (aa !== ba) return ba - aa;
    return (b.startedAt || 0) - (a.startedAt || 0);
  });
  const list = all.map((s) => sessionPublicView(s, rateFor(s)));

  let aggregateTps = 0;
  let activeSessions = 0;
  const activeTpsByModel = new Map();
  for (const s of list) {
    if (s.active) {
      activeSessions++;
      aggregateTps += s.tps;
      let arr = activeTpsByModel.get(s.model);
      if (!arr) { arr = []; activeTpsByModel.set(s.model, arr); }
      arr.push(s.tps);
    }
  }
  for (const arr of activeTpsByModel.values()) arr.sort((a, b) => a - b);

  const models = [...state.seenModels].map((model) => {
    const vals = tpsByModel.get(model);
    const live = activeTpsByModel.get(model);
    const ttft = ttftByModel.get(model);
    return {
      model,
      tps: live && live.length ? (percentile(live, 0.5) ?? 0) : 0,
      medianTps: percentile(vals, 0.5),
      p10Tps: percentile(vals, 0.1),
      ttftMsP50: percentile(ttft, 0.5),
    };
  }).sort((a, b) => b.tps - a.tps);

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
      tps: Math.round(aggregateTps * 10) / 10,
      activeSessions,
      totalSessions: list.length,
      medianTps: globalMedian,
      p10Tps: percentile(tpsValues, 0.1),
      ttftMsP50: percentile(ttftValues, 0.5),
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
    if (state.wsClients.size > 0) broadcastEvent('sessions', getSessionsSnapshot());
    return;
  }
  state.broadcastThrottled = true;
  setTimeout(() => {
    state.broadcastThrottled = false;
    if (state.wsClients.size > 0) broadcastEvent('sessions', getSessionsSnapshot());
    if (!state.sessionsBroadcastTimer) {
      state.sessionsBroadcastTimer = setInterval(() => {
        let stillActive = false;
        for (const s of state.sessions.values()) { if (s.status === 'active') { stillActive = true; break; } }
        if (state.wsClients.size > 0) broadcastEvent('sessions', getSessionsSnapshot());
        if (!stillActive && state.sessionsBroadcastTimer) {
          clearInterval(state.sessionsBroadcastTimer);
          state.sessionsBroadcastTimer = null;
        }
      }, state.SESSION_BROADCAST_MIN_MS).unref();
    }
  }, 0).unref();
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

// Hot reload can inherit a state singleton that predates these limits.
for (const [id, timer] of state.sessionTimers) {
  if (!state.sessions.has(id)) {
    clearTimeout(timer);
    state.sessionTimers.delete(id);
  }
}

enforceSessionHistoryLimit();
for (const [groupKey, ids] of state.sessionsByGroup) {
  if (!(ids instanceof Set)) { state.sessionsByGroup.delete(groupKey); continue; }
  for (const id of ids) { if (!state.sessions.has(id)) ids.delete(id); }
  if (ids.size === 0) state.sessionsByGroup.delete(groupKey);
}

while (state.groupSummaries.size > groupSummaryLimit()) reserveGroupSummarySlot();
while (state.seenModels.size > seenModelsLimit()) state.seenModels.delete(state.seenModels.values().next().value);
while (state.modelCharRatio.size > modelRatioLimit()) state.modelCharRatio.delete(state.modelCharRatio.keys().next().value);

module.exports = {
  estimateTokensFromChars,
  updateModelRatio,
  createSession,
  updateGroupSummary,
  finalizeSession,
  sessionTps,
  sessionPublicView,
  getSessionsSnapshot,
  scheduleSessionsBroadcast,
  broadcastEvent,
  enforceSessionHistoryLimit,
  DEFAULT_SESSION_HISTORY_LIMIT,
  DEFAULT_GROUP_SUMMARY_LIMIT,
  DEFAULT_SEEN_MODELS_LIMIT,
  DEFAULT_MODEL_RATIO_LIMIT,
};
