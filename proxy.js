const fs = require('fs');
const path = require('path');
const http = require('http');
const { spawn } = require('child_process');

const CONFIG_DIR = path.join(__dirname, '.config');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.json');
const DEFAULT_UPSTREAM = 'https://api.code.umans.ai/v1';
const MAX_BODY_SIZE = 25 * 1024 * 1024;
const USAGE_TTL_MS = 10 * 1000;
const MODEL_INFO_TTL_MS = 60 * 1000;

// Canonical reasoning-effort ordering, weakest to strongest.
// Covers OpenAI (minimal/low/medium/high), UMANS (none/low/medium/high/max),
// and common aliases clients send (xhi/xhigh -> max). Unknown values snap up
// to the nearest supported level so a "max" intent never silently downgrades.
const REASONING_RANK = { none: 0, off: 0, disabled: 0, minimal: 1, low: 2, medium: 3, high: 4, xhi: 5, xhigh: 5, max: 5 };

let config = loadConfig();
const startedAt = Date.now();
let usageCache = { data: null, time: 0 };
let concurrencyCache = { concurrent: null, limit: null, softLimit: null, user_id: null, time: 0 };
let activeRequests = 0;
let queuedRequests = 0;
const sseClients = new Set();
let modelInfoCache = { data: null, time: 0 };
// Per-session tracking for live TPS + session listing. Sessions are created
// when a chat request begins streaming/receiving and removed on completion.
// Bytes forwarded to the client are NEVER mutated; the tap only observes.
const sessions = new Map();
let sessionSeq = 0;
// Rolling aggregate TPS: tracks tokens emitted in the last 5s.
const TPS_WINDOW_MS = 5000;
const tpsBuckets = []; // { time, tokens }
// Per-model rolling TPS buckets + set of models we have actually used.
// The dashboard Overview breaks down TPS by model, surfacing only seen models.
const tpsBucketsByModel = new Map(); // model -> [{ time, tokens }]
const seenModels = new Set();
// Debug ring buffer: captures coalescing trace data for live inspection.
// Accessed via GET /api/debug/coalesce (requires proxy auth).
const coalesceDebug = [];
const COALESCE_DEBUG_MAX = 50;
function logCoalesce(entry) {
  coalesceDebug.push(entry);
  if (coalesceDebug.length > COALESCE_DEBUG_MAX) coalesceDebug.shift();
}
// Persistent conversation group summaries. Survive past individual session
// expiry (120s) so the dashboard's grouped view shows correct turn counts and
// aggregate stats even when older sessions have been cleaned up. Keyed by
// groupKey; entries expire after GROUP_SUMMARY_TTL_MS of inactivity.
const groupSummaries = new Map();
const GROUP_SUMMARY_TTL_MS = 5 * 60 * 1000; // match COALESCE_TTL_MS
let sessionsBroadcastTimer = null;
let broadcastThrottled = false;
const SESSION_BROADCAST_MIN_MS = 1000;

function newSessionId() {
  sessionSeq = (sessionSeq + 1) % 0x100000000;
  return Date.now().toString(36) + sessionSeq.toString(36).padStart(8, '0');
}

// Per-model learned char→token ratio, accumulated from completed requests
// that returned exact usage. Used to estimate tok/s DURING streaming
// (before the final usage chunk arrives). The rolling TPS bucket is
// NEVER fed from this estimate — only from the final exact count at _finish.
const modelCharRatio = new Map(); // model -> { chars, tokens }

function estimateTokensFromChars(model, chars) {
  const r = modelCharRatio.get(model);
  if (r && r.chars > 0) return Math.round(chars * (r.tokens / r.chars));
  // Cold start: 4 chars/token until the first exact-usage request teaches us.
  return Math.floor(chars / 4);
}

function updateModelRatio(model, chars, tokens) {
  if (!model || chars <= 0 || tokens <= 0) return;
  const r = modelCharRatio.get(model) || { chars: 0, tokens: 0 };
  r.chars += chars;
  r.tokens += tokens;
  modelCharRatio.set(model, r);
}

// ---- session coalescing: glue requests that share an upstream KV cache ----
// Conversations grow append-only; upstream caches by content-addressed prefix.
// We hash the message prefix to find the logical session a request belongs to.
// All hashing is FNV-1a (fast, non-crypto, zero-dep). The per-message hash
// cache avoids re-hashing large system prompts on every turn.
const messageHashCache = new Map(); // JSON.stringify(message) -> fnv1a hex
const stateMap = new Map();         // chainHash -> groupKey (logical session)
const COALESCE_TTL_MS = 5 * 60 * 1000; // match upstream cache lifetime

// FNV-1a 32-bit. Returns hex string. ~1ns/byte, no allocation beyond output.
function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, '0');
}

// Cached per-message hash. JSON.stringify is the expensive part; cache it so
// a system prompt reused across 50 turns is stringified+hashed exactly once.
// True LRU: on a hit, delete + re-insert to move to end (most-recently-used).
// Map iteration order is insertion order, so keys().next() returns the LRU.
// Normalize a message to its cache-relevant identity: role + content text.
// Extra fields (refusal, reasoning_content, tool_calls, provider_specific_fields,
// …) vary between what the proxy captures and what the client replays, so
// they MUST NOT participate in the prefix hash. content null → "" so that a
// reasoning-only turn (proxy saw "" , client sends null) still coalesces.
function canonicalMessage(message) {
  const content = message?.content;
  return {
    role: message?.role || '',
    content: typeof content === 'string' ? content : (content == null ? '' : JSON.stringify(content)),
  };
}
function messageHash(message) {
  const json = JSON.stringify(canonicalMessage(message));
  let h = messageHashCache.get(json);
  if (h !== undefined) {
    // Promote to most-recently-used by re-inserting.
    messageHashCache.delete(json);
    messageHashCache.set(json, h);
    return h;
  }
  h = fnv1a(json);
  messageHashCache.set(json, h);
  // Bound the cache: drop LRU if it grows beyond a few thousand entries.
  if (messageHashCache.size > 5000) {
    const lru = messageHashCache.keys().next().value;
    messageHashCache.delete(lru);
  }
  return h;
}

// Rolling chain hash over [model, ...messages]. Each step is H(prev, msgHash)
// — O(1) per message, O(n) per request (n = message count, not token count).
// Returns { hash, chain } so the caller can extend without re-walking.
function chainHash(model, messages, fromChain) {
  let chain = fromChain || fnv1a(model || '');
  for (let i = 0; i < messages.length; i++) {
    chain = fnv1a(chain + messageHash(messages[i]));
  }
  return chain;
}

// Resolve the logical session for an incoming request. Called at arrival,
// before createSession. Returns { groupKey, prefixChain } so storeStateKey
// can extend the chain without re-walking the prefix.
function resolveGroupKey(model, messages) {
  const prefix = messages.slice(0, -1);
  const prefixChain = chainHash(model, prefix);
  const hit = stateMap.get(prefixChain);
  const groupKey = hit || newSessionId();
  if (coalesceDebug.length < COALESCE_DEBUG_MAX) logCoalesce({ t: Date.now(), type: 'resolve', model, msgCount: messages.length, prefixMsgCount: prefix.length, prefixChain, stateMapHit: !!hit, groupKey, stateMapSize: stateMap.size });
  return { groupKey, prefixChain };
}

// Store the conversation state after a request completes, so the NEXT request
// whose prefix matches this state hits the stateMap and coalesces. Called at
// session completion — never in the streaming hot path. Accepts the saved
// prefixChain from resolveGroupKey to avoid re-walking the prefix.
function storeStateKey(model, messages, responseContent, groupKey, prefixChain) {
  const lastUser = messages[messages.length - 1];
  const responseMsg = { role: 'assistant', content: responseContent || '' };
  // Extend the saved prefix chain with the final user turn + the response.
  const stateChain = chainHash(model, [lastUser, responseMsg], prefixChain);
  stateMap.set(stateChain, groupKey);
  if (coalesceDebug.length < COALESCE_DEBUG_MAX) logCoalesce({ t: Date.now(), type: 'store', model, msgCount: messages.length, stateChain, prefixChain, groupKey, responseContentLen: (responseContent || '').length, responseContentPreview: (responseContent || '').slice(0, 100), lastUserMsg: JSON.stringify(lastUser).slice(0, 200), lastUserHash: messageHash(lastUser), responseMsgHash: messageHash(responseMsg) });
  setTimeout(() => { stateMap.delete(stateChain); }, COALESCE_TTL_MS).unref();
}

function createSession({ model, stream, groupKey }) {
  const id = newSessionId();
  if (model) seenModels.add(model);
  const session = {
    id, model, stream: !!stream,
    groupKey: groupKey || id,  // logical session; defaults to own id (new conversation)
    startedAt: Date.now(),
    status: 'active',        // active | done | error | aborted
    outputTokens: 0,         // running token count (exact if known)
    exactTokens: false,      // whether outputTokens is exact or estimated
    chars: 0,                // observed output chars (for estimation)
    bytes: 0,                // total bytes forwarded (request + response)
    // Token breakdown for per-session display:
    promptTokens: 0,         // total input (prompt) tokens
    cachedTokens: 0,         // input tokens served from cache
    completionTokens: 0,     // output (completion) tokens
    responseContent: '',    // accumulated assistant reply (for coalescing stateKey)
    finalTps: null,         // set at finalize: outputTokens / generation time (TTFT excluded)
    firstTokenAt: null,      // set on first content delta; finalTps excludes TTFT
  };
  sessions.set(id, session);
  return session;
}
// Accumulate per-group stats that survive past individual session expiry.
function updateGroupSummary(session) {
  if (!session.groupKey) return;
  const g = groupSummaries.get(session.groupKey) || {
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
    // Running average: avoid storing every value. Keep sum + count.
    g.finalTpsSum = (g.finalTpsSum || 0) + session.finalTps;
    g.finalTpsCount = (g.finalTpsCount || 0) + 1;
  }
  g.lastSeenAt = Date.now();
  groupSummaries.set(session.groupKey, g);
  setTimeout(() => {
    // Expire stale summaries. Only delete if no new turns arrived.
    const entry = groupSummaries.get(session.groupKey);
    if (entry && Date.now() - entry.lastSeenAt >= GROUP_SUMMARY_TTL_MS) {
      groupSummaries.delete(session.groupKey);
    }
  }, GROUP_SUMMARY_TTL_MS).unref();
}

function finalizeSession(session, status) {
  if (!session) return;
  session.status = status;
  session.endedAt = Date.now();
  // Record the true generation TPS for stats. Exclude TTFT (time from
  // request start to first token) so queue time + prefill doesn't dilute
  // the rate. If we never saw a token, finalTps stays null.
  if (session.outputTokens > 0 && session.firstTokenAt) {
    const genMs = Math.max(1, session.endedAt - session.firstTokenAt);
    session.finalTps = Math.round((session.outputTokens / genMs) * 1000 * 10) / 10;
  } else if (session.outputTokens > 0) {
    // No firstTokenAt (e.g. non-streaming with immediate body): fall back
    // to full elapsed so we still have a number, but it includes TTFT.
    const elapsed = Math.max(1, session.endedAt - session.startedAt);
    session.finalTps = Math.round((session.outputTokens / elapsed) * 1000 * 10) / 10;
  }
  updateGroupSummary(session);
  // Keep the entry so the dashboard can show completed sessions and group
  // multi-turn conversations. 120s covers typical inter-turn gaps; the
  // stateMap (5 min TTL) handles prefix→groupKey coalescing independently.
  setTimeout(() => { sessions.delete(session.id); scheduleSessionsBroadcast(); }, 120000);
  scheduleSessionsBroadcast();
}

// Feed tokens into the aggregate and per-model rolling TPS counters.
// Callers own session.outputTokens — this function only touches the TPS
// buckets, never the session's token count (it used to, which double-counted
// on the exact path where callers set outputTokens before calling this).
function addSessionTokens(session, tokens, { exact = false } = {}) {
  if (!session) return;
  if (exact) session.exactTokens = true;
  if (tokens <= 0) return;
  const now = Date.now();
  const cutoff = now - TPS_WINDOW_MS;
  // Aggregate bucket.
  tpsBuckets.push({ time: now, tokens });
  while (tpsBuckets.length && tpsBuckets[0].time < cutoff) tpsBuckets.shift();
  // Per-model bucket.
  const model = session.model;
  if (model) {
    seenModels.add(model);
    let buckets = tpsBucketsByModel.get(model);
    if (!buckets) { buckets = []; tpsBucketsByModel.set(model, buckets); }
    buckets.push({ time: now, tokens });
    while (buckets.length && buckets[0].time < cutoff) buckets.shift();
  }
}
function aggregateTps() {
  const now = Date.now();
  const cutoff = now - TPS_WINDOW_MS;
  let tokens = 0;
  for (const b of tpsBuckets) if (b.time >= cutoff) tokens += b.tokens;
  // tokens accumulated over TPS_WINDOW_MS → tokens/sec
  return tokens / (TPS_WINDOW_MS / 1000);
}

// Per-model rolling TPS. Returns a map of model -> tokens/sec for models
// we have actually seen (used), sorted by descending TPS.
function modelTpsBreakdown() {
  const now = Date.now();
  const cutoff = now - TPS_WINDOW_MS;
  const out = [];
  for (const model of seenModels) {
    const buckets = tpsBucketsByModel.get(model);
    if (!buckets) { out.push({ model, tps: 0 }); continue; }
    let tokens = 0;
    for (const b of buckets) if (b.time >= cutoff) tokens += b.tokens;
    out.push({ model, tps: tokens / (TPS_WINDOW_MS / 1000) });
  }
  out.sort((a, b) => b.tps - a.tps);
  return out;
}

function sessionTps(session) {
  // Estimate per-session TPS from elapsed active time. For active sessions,
  // use the rolling char/token rate over the session lifetime (capped to 5s).
  if (!session) return 0;
  const now = Date.now();
  const end = session.endedAt ?? now;
  const elapsed = Math.max(1, end - session.startedAt);
  // Use a 5s window for a smoother rate.
  const window = Math.min(elapsed, TPS_WINDOW_MS);
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
    // Token breakdown for per-session display.
    promptTokens: session.promptTokens,
    cachedTokens: session.cachedTokens,
    uncachedTokens: Math.max(0, session.promptTokens - session.cachedTokens),
    completionTokens: session.completionTokens,
    // Cache hit rate: cached / total prompt tokens. Null when no prompt tokens.
    cacheHitRate: session.promptTokens > 0 ? Math.round((session.cachedTokens / session.promptTokens) * 1000) / 10 : null,
    bytes: session.bytes,
    // Active sessions show the live rolling rate; completed sessions show
    // finalTps (true generation rate, TTFT excluded) — never the 5s-capped
    // rolling rate which inflates short generations.
    tps: Math.round((!active && session.finalTps != null ? session.finalTps : sessionTps(session)) * 10) / 10,
    finalTps: session.finalTps,
  };
}

function getSessionsSnapshot() {
  const list = [...sessions.values()].map(sessionPublicView);
  // Active first, then most recent.
  list.sort((a, b) => (a.active === b.active ? b.startedAt.localeCompare(a.startedAt) : a.active ? -1 : 1));
  // Per-session TPS distribution stats from completed sessions only.
  // Uses finalTps (true generation rate, TTFT excluded) — never the live
  // rolling tps of active sessions. Null when no completed sessions yet.
  const tpsValues = list.filter((s) => !s.active && s.finalTps != null && s.finalTps > 0)
    .map((s) => s.finalTps).sort((a, b) => a - b);
  const percentile = (arr, p) => {
    if (!arr.length) return null;
    const idx = Math.min(arr.length - 1, Math.max(0, Math.floor((arr.length - 1) * p)));
    return Math.round(arr[idx] * 10) / 10;
  };
  const medianTps = percentile(tpsValues, 0.5);
  const p10Tps = percentile(tpsValues, 0.1);
  // Group summaries: persistent stats per conversation, surviving past
  // individual session expiry. The dashboard merges these with live sessions
  // for the grouped view so turn counts stay accurate when older sessions
  // have been cleaned up.
  const groups = [...groupSummaries.values()].map((g) => {
    const avgFinal = g.finalTpsCount
      ? Math.round(g.finalTpsSum / g.finalTpsCount * 10) / 10
      : null;
    return {
      groupKey: g.groupKey,
      model: g.model,
      turnCount: g.turnCount,
      totalTokens: g.totalTokens,
      totalCached: g.totalCached,
      totalUncached: g.totalUncached,
      totalBytes: g.totalBytes,
      avgFinalTps: avgFinal,
      lastSeenAt: g.lastSeenAt,
    };
  });
  return {
    sessions: list,
    groupSummaries: groups,
    aggregate: {
      tps: Math.round(aggregateTps() * 10) / 10,
      activeSessions: (() => { let n = 0; for (const s of sessions.values()) if (s.status === 'active') n++; return n; })(),
      medianTps,
      p10Tps,
      // Per-model TPS for models actually used, sorted desc by tps.
      models: modelTpsBreakdown().map((m) => ({ model: m.model, tps: Math.round(m.tps * 10) / 10 })),
    },
  };
}

// Broadcast sessions snapshot on a throttle (at most once per second) while
// sessions are active. Stops the timer when no active sessions remain.
function scheduleSessionsBroadcast() {
  if (broadcastThrottled) return;
  let hasActive = false;
  for (const s of sessions.values()) { if (s.status === 'active') { hasActive = true; break; } }
  if (!hasActive) {
    if (sessionsBroadcastTimer) { clearInterval(sessionsBroadcastTimer); sessionsBroadcastTimer = null; }
    // One final broadcast so the UI sees the terminal state.
    broadcastEvent('sessions', getSessionsSnapshot());
    return;
  }
  setTimeout(() => {
    broadcastThrottled = false;
    broadcastEvent('sessions', getSessionsSnapshot());
    // Re-evaluate; if still active, keep polling via interval.
    if (!sessionsBroadcastTimer) {
      sessionsBroadcastTimer = setInterval(() => {
        let stillActive = false;
        for (const s of sessions.values()) { if (s.status === 'active') { stillActive = true; break; } }
        broadcastEvent('sessions', getSessionsSnapshot());
        if (!stillActive && sessionsBroadcastTimer) {
          clearInterval(sessionsBroadcastTimer);
          sessionsBroadcastTimer = null;
        }
      }, SESSION_BROADCAST_MIN_MS);
    }
  }, 0);
}

// Per-request stream/body tap. Parses SSE chunks (streaming) or a single
// JSON body (non-streaming) to count output tokens WITHOUT mutating bytes.
// ASYNC: onChunk is O(1) — it pushes the raw buffer onto a queue and
// schedules a drain via setImmediate. All JSON.parse / string scanning
// happens in _drain(), which runs between I/O ticks (during the next
// reader.read() await), so telemetry never delays bytes reaching the client.
class ChatTap {
  constructor(session, { stream, onFinalize }) {
    this.session = session;
    this.stream = stream;
    this.onFinalize = onFinalize || null; // called from _finish after responseContent is set
    this.lineBuf = '';     // partial SSE line buffer
    this.bodyBuf = [];     // non-streaming: full body chunks
    this.queue = [];       // raw chunks pending async parse
    this.draining = false;
    this.ended = false;
  }

  // O(1) enqueue — never blocks the read loop. Byte counting is trivial
  // integer addition; parsing is deferred to _drain().
  onChunk(buf) {
    if (!this.session || !buf || buf.length === 0) return;
    this.session.bytes += buf.length;
    this.queue.push(buf);
    if (!this.draining) {
      this.draining = true;
      setImmediate(() => this._drain());
    }
  }

  _drain() {
    while (this.queue.length) {
      const buf = this.queue.shift();
      if (this.stream) {
        // reader.read() returns Uint8Array, not Buffer. Uint8Array.toString('utf8')
        // returns comma-separated byte values, so wrap in Buffer.from first.
        this.lineBuf += Buffer.from(buf).toString('utf8');
        let idx;
        while ((idx = this.lineBuf.indexOf('\n')) >= 0) {
          const line = this.lineBuf.slice(0, idx).replace(/\r$/, '');
          this.lineBuf = this.lineBuf.slice(idx + 1);
          this._handleSseLine(line);
        }
      } else {
        this.bodyBuf.push(buf);
      }
    }
    this.draining = false;
    // If onEnd arrived while draining, finalize now that we're caught up.
    if (this.ended) this._finish();
  }

  _handleSseLine(line) {
    if (!line.startsWith('data:')) return;
    const data = line.slice(5).trim();
    if (!data || data === '[DONE]') return;
    let chunk;
    try { chunk = JSON.parse(data); } catch { return; }
    // Exact usage on the final chunk (some providers include it). Once we
    // have an exact count, stop estimating from chars.
    const u = chunk?.usage;
    if (u && (u.completion_tokens != null || u.output_tokens != null)) {
      const t = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
      // Feed the rolling TPS bucket ONLY here, at final-usage time, with
      // the delta between exact and whatever the interim estimate held.
      // The bucket is never fed from interim char estimates.
      const prev = this.session.outputTokens;
      this.session.outputTokens = t;
      this.session.completionTokens = t;
      this.session.exactTokens = true;
      const prompt = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
      const cached = Number(u.prompt_tokens_details?.cached_tokens ?? u.cached_tokens ?? u.tokens_cached_read ?? 0) || 0;
      this.session.promptTokens = prompt;
      this.session.cachedTokens = cached;
      // Learn the real char→token ratio for this model from exact data.
      updateModelRatio(this.session.model, this.session.chars, t);
      addSessionTokens(this.session, Math.max(0, t - prev), { exact: true });
      return;
    }
    if (this.session.exactTokens) return; // already have exact count
    // Interim: accumulate chars and estimate outputTokens for the live
    // readout. This does NOT touch the TPS bucket — that happens only at
    // _finish / final-usage time with the exact count.
    let chars = 0;
    const choices = chunk?.choices;
    if (Array.isArray(choices)) {
      for (const c of choices) {
        const delta = c?.delta || {};
        if (typeof delta.content === 'string') {
          chars += delta.content.length;
          this.session.responseContent += delta.content;
        }
        if (typeof delta.reasoning_content === 'string') chars += delta.reasoning_content.length;
      }
    }
    if (chars > 0) {
      if (!this.session.firstTokenAt) this.session.firstTokenAt = Date.now();
      this.session.chars += chars;
      this.session.outputTokens = estimateTokensFromChars(this.session.model, this.session.chars);
    }
  }

  onEnd() {
    if (!this.session) return;
    this.ended = true;
    // If a drain is scheduled (setImmediate pending) but hasn't run yet, the
    // queue still holds unparsed chunks. Drain them synchronously now so the
    // final token count is accurate before the session is finalized. If a
    // drain IS in flight (executing), _drain will call _finish when caught up.
    if (!this.draining && this.queue.length) this._drain();
    else if (!this.draining) this._finish();
  }

  _finish() {
    if (!this.session) return;
    // Flush any straggler SSE line without a trailing newline.
    if (this.stream && this.lineBuf) {
      const line = this.lineBuf.trim();
      this.lineBuf = '';
      if (line) this._handleSseLine(line);
    }
    if (!this.stream && this.bodyBuf.length) {
      // Non-streaming: parse the buffered body once for exact usage.
      const text = Buffer.concat(this.bodyBuf).toString('utf8');
      try {
        const body = JSON.parse(text);
        const u = body?.usage;
        if (u && (u.completion_tokens != null || u.output_tokens != null)) {
          // Replace any estimate with the exact count from the final body.
          const exact = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
          const prev = this.session.outputTokens;
          this.session.outputTokens = exact;
          this.session.completionTokens = exact;
          this.session.exactTokens = true;
          const prompt = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
          const cached = Number(u.prompt_tokens_details?.cached_tokens ?? u.cached_tokens ?? u.tokens_cached_read ?? 0) || 0;
          this.session.promptTokens = prompt;
          this.session.cachedTokens = cached;
          // Learn the real char→token ratio for this model from exact data.
          updateModelRatio(this.session.model, this.session.chars, exact);
          // Accumulate response content for coalescing (non-streaming).
          const choices = body?.choices;
          if (Array.isArray(choices)) {
            for (const c of choices) {
              const msg = c?.message || {};
              if (typeof msg.content === 'string') this.session.responseContent += msg.content;
            }
          }
          addSessionTokens(this.session, Math.max(0, exact - prev), { exact: true });
        } else {
          // No usage block: estimate from total content chars and feed the
          // bucket once at finish time (interim never touched it).
          let chars = 0;
          const choices = body?.choices;
          if (Array.isArray(choices)) {
            for (const c of choices) {
              const msg = c?.message || {};
              if (typeof msg.content === 'string') {
                chars += msg.content.length;
                this.session.responseContent += msg.content;
              }
              if (typeof msg.reasoning_content === 'string') chars += msg.reasoning_content.length;
            }
          }
          if (chars > 0) {
            this.session.chars += chars;
            const est = estimateTokensFromChars(this.session.model, this.session.chars);
            const prev = this.session.outputTokens;
            this.session.outputTokens = est;
            addSessionTokens(this.session, Math.max(0, est - prev));
          }
        }
      } catch { /* non-JSON or partial — ignore */ }
    }
    // Coalesce: store the conversation state now that responseContent is
    // fully populated. Runs inside _finish (which may be deferred by an
    // in-flight drain) so the state is always complete when stored.
    if (this.onFinalize) { try { this.onFinalize(this.session); } catch {} }
    // Clear ref so finalize is idempotent.
    this.session = null;
  }
}



function readJSON(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (err) {
    if (err.code === 'ENOENT') return {};
    console.error(`Failed to load ${file}: ${err.message}`);
    process.exit(1);
  }
}

function cleanKeys(values) {
  return [...new Set(values.map((key) => String(key || '').trim()).filter(Boolean))];
}

function fileProxyApiKeys(raw) {
  return cleanKeys(Array.isArray(raw.API_KEYS) ? raw.API_KEYS.map((entry) => typeof entry === 'string' ? entry : entry?.key) : []);
}

function envProxyApiKeys() {
  return cleanKeys((process.env.API_KEYS || '').split(','));
}

function parseDuration(value) {
  const raw = String(value || '15m').trim().toLowerCase();
  const match = raw.match(/^(\d+)(ms|s|m|h)?$/);
  if (!match) throw new Error('REQUEST_TIMEOUT must be like 30000ms, 30s, 15m, or 1h');
  const n = Number(match[1]);
  const unit = match[2] || 'ms';
  const scale = unit === 'h' ? 3600000 : unit === 'm' ? 60000 : unit === 's' ? 1000 : 1;
  return n * scale;
}

function loadConfig() {
  const raw = readJSON(CONFIG_FILE);
  const envApiKey = process.env.UMANS_API_KEY || '';
  const fileApiKey = raw.API_KEY || '';
  const fileKeys = fileProxyApiKeys(raw);
  const requestTimeoutRaw = process.env.REQUEST_TIMEOUT || raw.REQUEST_TIMEOUT || '15m';
  return {
    listenAddr: process.env.LISTEN_ADDR || raw.LISTEN_ADDR || '127.0.0.1:8084',
    upstreamBaseURL: DEFAULT_UPSTREAM,
    apiKey: envApiKey || fileApiKey,
    fileApiKey,
    enabledModels: Array.isArray(raw.ENABLED_MODELS) ? raw.ENABLED_MODELS.map(String).map((x) => x.trim()).filter(Boolean) : [],
    requestTimeout: parseDuration(requestTimeoutRaw),
    requestTimeoutRaw,
    overrideConcurrency: Math.max(0, Number(process.env.OVERRIDE_CONCURRENCY || raw.OVERRIDE_CONCURRENCY || 0) || 0),
    proxyApiKeys: cleanKeys([...envProxyApiKeys(), ...fileKeys]),
    fileProxyApiKeys: fileKeys,
  };
}

function saveConfig(next = config) {
  if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true, mode: 0o700 });
  fs.chmodSync(CONFIG_DIR, 0o700);
  fs.writeFileSync(CONFIG_FILE, JSON.stringify({
    LISTEN_ADDR: next.listenAddr,
    API_KEY: next.fileApiKey,
    ENABLED_MODELS: next.enabledModels,
    API_KEYS: next.fileProxyApiKeys,
    REQUEST_TIMEOUT: next.requestTimeoutRaw,
    OVERRIDE_CONCURRENCY: next.overrideConcurrency,
  }, null, 2) + '\n', { mode: 0o600 });
  fs.chmodSync(CONFIG_FILE, 0o600);
}

function parseListenAddr(value) {
  const raw = String(value || '').trim();
  if (!raw) return { host: '127.0.0.1', port: 8084 };

  const match = raw.match(/^(.+):(\d+)$/);
  const portText = match ? match[2] : raw;
  if (!match && raw.includes(':')) throw new Error('LISTEN_ADDR must be a port or host:port');

  const port = Number(portText);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('LISTEN_ADDR port must be an integer from 1 to 65535');
  return { host: match ? match[1] : '127.0.0.1', port };
}

function writeJSON(res, status, body, headers = {}) {
  const data = Buffer.from(JSON.stringify(body, null, 2));
  res.writeHead(status, { ...headers, 'Content-Type': 'application/json', 'Content-Length': data.length });
  res.end(data);
}

function writeText(res, status, body, contentType = 'text/plain; charset=utf-8') {
  const data = Buffer.from(body);
  res.writeHead(status, { 'Content-Type': contentType, 'Content-Length': data.length });
  res.end(data);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;
    let settled = false;
    const finish = (fn) => { if (settled) return; settled = true; req.off('close', onClose); fn(); };
    const onClose = () => finish(() => reject(new Error('client disconnected')));
    req.on('data', (chunk) => {
      if (tooLarge) return;
      size += chunk.length;
      if (size > MAX_BODY_SIZE) {
        tooLarge = true;
        const err = new Error('request body too large');
        err.statusCode = 413;
        finish(() => reject(err));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => finish(() => resolve(Buffer.concat(chunks).toString('utf8'))));
    req.on('error', (err) => finish(() => reject(err)));
    req.on('close', onClose);
  });
}

function upstreamURL(suffix) {
  return `${config.upstreamBaseURL}${suffix}`;
}

function authHeaders(extra = {}) {
  const headers = { ...extra };
  if (config.apiKey) headers.Authorization = `Bearer ${config.apiKey}`;
  return headers;
}

function safeHeaders(headers) {
  const out = {};
  for (const [key, value] of headers) {
    const lower = key.toLowerCase();
    // Strip hop-by-hop headers; keep content-length since we forward the
    // upstream body byte-for-byte (both streaming and non-streaming).
    if (['connection', 'content-encoding', 'keep-alive', 'transfer-encoding'].includes(lower)) continue;
    out[key] = value;
  }
  return out;
}

function openAIError(res, status, message, type = 'server_error') {
  writeJSON(res, status, { error: { message, type, param: null, code: null } });
}

// Authorization check for proxy-protected endpoints.
// NOTE: auth is intentionally DISABLED by default. With an empty API_KEYS
// list every endpoint is open. This is the chosen default for a localhost
// single-user tool; set API_KEYS (env or config) to gate access when binding
// to a non-loopback address. Keep this behavior explicit, not accidental.
function authorized(req, url) {
  if (!config.proxyApiKeys.length) return true;
  const tokens = [req.headers['x-api-key'], req.headers.authorization, url?.searchParams.get('key')]
    .flatMap((value) => Array.isArray(value) ? value : [value])
    .map((value) => String(value || '').replace(/^Bearer\s+/i, '').trim())
    .filter(Boolean);
  return tokens.some((token) => config.proxyApiKeys.includes(token));
}

function requiresProxyAuth(pathname) {
  return pathname === '/api/config' || pathname === '/api/events' || pathname === '/api/shutdown' || pathname === '/api/restart' || pathname === '/api/clear-state' || pathname === '/api/system/info' || pathname === '/api/debug/coalesce' || pathname.startsWith('/api/umans/') || pathname.startsWith('/v1/');
}

function filterModels(models) {
  if (!config.enabledModels.length) return models;
  const allowed = new Set(config.enabledModels);
  return models.filter((model) => allowed.has(model.id || model));
}

function firstNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function concurrencyHardLimit(concurrency) {
  const soft = firstNumber(concurrency.limit);
  const hard = firstNumber(concurrency.hard_cap);
  if (hard) return hard;
  const burst = firstNumber(concurrency.burst, concurrency.burst_limit, concurrency.burst_sessions);
  if (soft && burst) return soft + burst;
  const burstPct = firstNumber(concurrency.burst_pct);
  if (soft && burstPct) return Math.ceil(soft * (1 + burstPct));
  const burstPercent = firstNumber(concurrency.burst_percent);
  if (soft && burstPercent) return Math.ceil(soft * (1 + burstPercent / 100));
  return soft;
}

function percentValue(...values) {
  for (const value of values) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) continue;
    return Math.max(0, Math.min(1, n > 1 ? n / 100 : n));
  }
  return null;
}

function burstQuota(concurrency) {
  return percentValue(
    concurrency.burst_remaining_pct,
    concurrency.burst_pct_remaining,
    concurrency.remaining_burst_pct,
    concurrency.burst_available_pct,
    concurrency.burst_remaining_percent,
    concurrency.remaining_burst_percent,
    concurrency.burst_percent_remaining,
    concurrency.burst_pct,
    concurrency.burst_percent,
  ) ?? 0;
}

function concurrencyQuotaLimit(concurrency) {
  const soft = firstNumber(concurrency.limit);
  const hard = concurrencyHardLimit(concurrency);
  if (!soft || !hard || hard <= soft) return hard ?? soft;
  return Math.max(soft, Math.min(hard, soft + Math.floor((hard - soft) * burstQuota(concurrency))));
}

function applyOverride(apiLimit, apiSoft, override) {
  if (override > 0) {
    return {
      limit: apiLimit != null ? Math.min(override, apiLimit) : override,
      softLimit: apiSoft != null ? Math.min(override, apiSoft) : null,
      overridden: apiLimit === null || override < apiLimit,
    };
  }
  return { limit: apiLimit, softLimit: apiSoft, overridden: false };
}

function extractThrottle(data) {
  const usage = data?.usage || data || {};
  const limits = data?.limits || usage?.limits || {};
  const concurrency = limits?.concurrency || {};
  const concurrent = Number(usage.concurrent_sessions ?? usage.concurrent ?? data?.concurrent_sessions ?? 0) || 0;
  const soft = firstNumber(concurrency.limit);
  const hard = concurrencyHardLimit(concurrency);
  const quotaLimit = concurrencyQuotaLimit(concurrency);
  const { limit, softLimit, overridden } = applyOverride(quotaLimit, soft, config.overrideConcurrency || 0);
  return {
    concurrent, soft, hard, softLimit, limit, quotaLimit, overridden,
    burstQuota: burstQuota(concurrency),
    active: activeRequests, queued: queuedRequests,
  };
}

function logError(context, err) {
  console.error(`${context}: ${err?.message || err}`);
}

async function fetchUmansUsage({ force = false } = {}) {
  if (!config.apiKey) return { ok: false, error: 'UMANS API key is not configured', usage: null, limits: null, throttle: extractThrottle(null) };
  if (!force && usageCache.data && Date.now() - usageCache.time < USAGE_TTL_MS) return usageCache.data;
  try {
    const resp = await fetch(upstreamURL('/usage'), {
      headers: authHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) throw new Error(`UMANS /usage returned ${resp.status}`);
    const raw = await resp.json();
    const result = {
      ok: true,
      raw,
      usage: raw?.usage ?? raw ?? null,
      limits: raw?.limits ?? null,
      user_id: raw?.user_id ?? raw?.user?.id ?? null,
      plan: raw?.plan ?? null,
      window: raw?.window ?? raw?.usage?.window ?? null,
      throttle: extractThrottle(raw),
      fetchedAt: new Date().toISOString(),
    };
    usageCache = { data: result, time: Date.now() };
    concurrencyCache = {
      concurrent: Number(raw?.usage?.concurrent_sessions ?? 0) || 0,
      limit: concurrencyQuotaLimit(raw?.limits?.concurrency || {}),
      softLimit: firstNumber(raw?.limits?.concurrency?.limit),
      user_id: raw?.user_id ?? null,
      time: Date.now(),
    };
    broadcastEvent('usage', result);
    return result;
  } catch (err) {
    logError('UMANS /usage fetch failed', err);
    return { ok: false, error: err.message, usage: null, limits: null, throttle: extractThrottle(null) };
  }
}

let refreshUsageInFlight = false;
let refreshUsageTimer = null;
const REFRESH_USAGE_MIN_MS = 10000; // coalesce: at most one forced /usage per 10s
function refreshUsageSoon() {
  // Coalesce: if a refresh is in flight or ran recently, skip. The 10s TTL
  // cache means a fresh fetch this soon would return the same data anyway.
  if (refreshUsageInFlight) return;
  if (refreshUsageTimer) return;
  if (usageCache.data && Date.now() - usageCache.time < REFRESH_USAGE_MIN_MS) return;
  refreshUsageInFlight = true;
  refreshUsageTimer = setTimeout(() => { refreshUsageTimer = null; }, REFRESH_USAGE_MIN_MS);
  fetchUmansUsage({ force: true })
    .catch(() => {})
    .finally(() => { refreshUsageInFlight = false; });
}

function broadcastEvent(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

function getEffectiveConcurrency() {
  const { limit, softLimit, overridden } = applyOverride(
    concurrencyCache.limit,
    concurrencyCache.softLimit,
    config.overrideConcurrency || 0,
  );
  return { concurrent: concurrencyCache.concurrent || 0, limit, softLimit, overridden, user_id: concurrencyCache.user_id || null };
}
function canStart(effective) {
  const limit = effective.limit;
  if (limit == null) return true;
  // Use the upstream-reported concurrent_sessions as a floor so external tools
  // hitting the same account are counted. The upstream count is polled and
  // stale (up to USAGE_TTL_MS), so this prevents gross overage, not bursts.
  const known = Math.max(activeRequests, effective.concurrent || 0);
  if (known >= limit) return false;
  // Prefer staying at/under the soft limit when requests are queued
  const soft = effective.softLimit;
  if (soft && known >= soft && queuedRequests > 0) return false;
  return true;
}

async function acquireThrottleSlot(res, signal, { keepalive } = {}) {
  if (signal?.aborted) throw new Error('aborted');
  let effective = getEffectiveConcurrency();
  // Cold cache: if the concurrency limit is unknown, we need it before
  // starting MORE requests — but the very first request (no active) can
  // never exceed any limit, so let it through immediately and populate
  // the cache in the background. If active requests exist, block on the
  // fetch so we don't burst past the limit.
  if (effective.limit === null) {
    if (activeRequests === 0) refreshUsageSoon();
    else { await fetchUmansUsage(); effective = getEffectiveConcurrency(); }
  }
  // While queued, emit SSE comment frames to keep omp's idle watchdog alive.
  // omp's iterateWithIdleTimeout counts SSE events; comment frames reset it.
  // This only applies to streaming requests where we've already sent 200 + SSE
  // headers. Non-streaming requests just wait (omp's timeout:false covers them).
  let keepaliveTimer = null;
  try {
    while (!canStart(effective)) {
      if (signal?.aborted) throw new Error('aborted');
      // Start keepalive on first queue iteration if the caller provided one.
      if (!keepaliveTimer && keepalive) keepaliveTimer = setInterval(keepalive, 3000);
      queuedRequests++;
      try {
        await new Promise((resolve, reject) => {
          const onAbort = () => { cleanup(); reject(new Error('aborted')); };
          const cleanup = () => {
            clearTimeout(timeout);
            signal?.removeEventListener('abort', onAbort);
          };
          const timeout = setTimeout(() => { cleanup(); resolve(); }, 1000);
          signal?.addEventListener('abort', onAbort, { once: true });
          if (signal?.aborted) onAbort();
        });
      } finally {
        queuedRequests--;
      }
      // Re-read the effective limit each iteration so config changes or usage
      // refreshes (POST /api/config resets concurrencyCache) are honored by
      // queued requests instead of evaluated against a stale snapshot.
      effective = getEffectiveConcurrency();
    }
  } finally {
    if (keepaliveTimer) clearInterval(keepaliveTimer);
  }
  if (signal?.aborted) throw new Error('aborted');
  activeRequests++;
  refreshUsageSoon();
  broadcastEvent('session', { type: 'start', active: activeRequests, queued: queuedRequests });
}

function releaseThrottleSlot() {
  if (activeRequests > 0) activeRequests--;
  refreshUsageSoon();
  broadcastEvent('session', { type: 'end', active: activeRequests, queued: queuedRequests });
}

// Fetch and cache upstream /models/info (per-model reasoning capabilities).
// Used to enrich /v1/models and to snap client reasoning_effort values.
async function fetchModelInfo({ force = false } = {}) {
  if (!force && modelInfoCache.data && Date.now() - modelInfoCache.time < MODEL_INFO_TTL_MS) return modelInfoCache.data;
  try {
    const resp = await fetch(upstreamURL('/models/info'), {
      headers: authHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) throw new Error(`/models/info returned ${resp.status}`);
    const data = await resp.json();
    modelInfoCache = { data, time: Date.now() };
    return data;
  } catch (err) {
    logError('UMANS /models/info fetch failed', err);
    return modelInfoCache.data; // serve stale if available, else null
  }
}

// Resolve a model's reasoning spec from cached /models/info. Returns null when
// the model is unknown or upstream /models/info is unavailable.
function getModelReasoning(modelId) {
  const info = modelInfoCache.data;
  if (!info || !modelId) return null;
  const entry = info[modelId];
  return entry?.capabilities?.reasoning || null;
}

// Map a client-supplied reasoning_effort to one the model actually supports.
// Unknown aliases (xhi, xhigh, minimal, off, disabled) normalize first, then
// snap UP to the nearest supported level so a "max" intent never downgrades.
// Models that can't disable reasoning (e.g. kimi-k2.7) drop a "none"/"off".
// Returns the resolved level, or null to leave the field untouched.
function snapReasoningLevel(modelId, requested) {
  const reasoning = getModelReasoning(modelId);
  if (!reasoning || !reasoning.supported) return null;
  const levels = (reasoning.levels || []).map((l) => String(l).toLowerCase());
  if (!requested) return null;
  const req = String(requested).toLowerCase().trim();

  // Model lists its supported levels: snap to nearest.
  if (levels.length) {
    if (levels.includes(req)) return requested; // already supported (preserve original casing)
    const reqRank = REASONING_RANK[req];
    if (reqRank == null) return null; // genuinely unknown — let upstream handle it
    const ranked = levels
      .map((l) => ({ l, r: REASONING_RANK[l] ?? -1 }))
      .filter((x) => x.r >= 0) // drop any level we can't rank
      .filter((x) => !(!reasoning.can_disable && x.r === 0)); // skip "none" when not disableable
    if (!ranked.length) return null;
    // Snap UP to the nearest supported rank >= requested so a "max" intent
    // never downgrades. If nothing reaches the requested rank, clamp to the
    // highest available (the strongest reasoning the model can do).
    const above = ranked.filter((x) => x.r >= reqRank).sort((a, b) => a.r - b.r);
    const chosen = above.length ? above[0] : ranked.reduce((m, x) => (x.r > m.r ? x : m));
    return chosen.l;
  }

  // Model supports reasoning but publishes no level list (e.g. kimi). Drop a
  // disable attempt (it can't), otherwise pass through unchanged.
  if (!reasoning.can_disable && (req === 'none' || req === 'off' || req === 'disabled')) return null;
  return requested;
}

// Attach a `reasoning` object (OpenAI-style) to each model in a /v1/models
// response, derived from cached /models/info.
function enrichModelsWithReasoning(data) {
  if (!Array.isArray(data)) return data;
  return data.map((m) => {
    const reasoning = getModelReasoning(m.id);
    if (!reasoning || !reasoning.supported) return m;
    return {
      ...m,
      reasoning: {
        supported: true,
        can_disable: reasoning.can_disable,
        levels: reasoning.levels || [],
        default_level: reasoning.default_level,
      },
    };
  });
}

async function handleModels(req, res, modelId = null) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  if (modelId && config.enabledModels.length && !config.enabledModels.includes(modelId)) {
    return openAIError(res, 404, `model not found: ${modelId}`, 'invalid_request_error');
  }
  if (!config.apiKey) {
    if (!modelId) return writeJSON(res, 200, { object: 'list', data: enrichModelsWithReasoning(config.enabledModels.map((id) => ({ id, object: 'model', created: 0, owned_by: 'umans' }))) });
    const model = config.enabledModels.find((id) => id === modelId);
    if (model) return writeJSON(res, 200, enrichModelsWithReasoning([{ id: model, object: 'model', created: 0, owned_by: 'umans' }])[0]);
    return openAIError(res, 404, `model not found: ${modelId}`, 'invalid_request_error');
  }

  // Best-effort: refresh model info so reasoning enrichment is current.
  await fetchModelInfo();

  try {
    const upstream = await fetch(upstreamURL(modelId ? `/models/${encodeURIComponent(modelId)}` : '/models'), { headers: authHeaders({ Accept: 'application/json' }) });
    const text = await upstream.text();
    let body;
    try { body = text ? JSON.parse(text) : {}; }
    catch { return writeText(res, upstream.status, text, upstream.headers.get('content-type') || 'text/plain'); }
    if (!modelId && Array.isArray(body.data)) body.data = enrichModelsWithReasoning(filterModels(body.data));
    if (modelId && Array.isArray(body.data)) body.data = enrichModelsWithReasoning(body.data);
    if (modelId && upstream.status === 404) return openAIError(res, 404, `model not found: ${modelId}`, 'invalid_request_error');
    writeJSON(res, upstream.status, body, safeHeaders(upstream.headers));
  } catch (err) {
    openAIError(res, 502, err.message);
  }
}

async function handleModelsInfo(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  if (!config.apiKey) return openAIError(res, 400, 'UMANS API key is not configured', 'invalid_request_error');
  try {
    const data = await fetchModelInfo({ force: new URL(req.url, 'http://localhost').searchParams.get('force') === '1' });
    if (!data) return openAIError(res, 502, 'upstream /models/info unavailable');
    writeJSON(res, 200, data);
  } catch (err) {
    openAIError(res, 502, err.message);
  }
}

function writeChunk(res, chunk, signal) {
  if (signal?.aborted) return Promise.reject(new Error('aborted'));
  if (res.write(chunk)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      res.off('drain', onDrain);
      res.off('close', onClose);
      res.off('error', onError);
      signal?.removeEventListener('abort', onAbort);
    };
    const onDrain = () => { cleanup(); resolve(); };
    const onClose = () => { cleanup(); resolve(); };
    const onError = (err) => { cleanup(); reject(err); };
    const onAbort = () => { cleanup(); reject(new Error('aborted')); };
    res.once('drain', onDrain);
    res.once('close', onClose);
    res.once('error', onError);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

async function handleChat(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  if (!config.apiKey) return openAIError(res, 400, 'UMANS API key is not configured', 'invalid_request_error');

  // Read the raw body once. We parse it for validation + telemetry, but
  // forward the original bytes to upstream unless we actually mutated the
  // payload — avoiding a redundant JSON.stringify of a potentially huge body.
  let rawBody;
  try { rawBody = await readBody(req); }
  catch (err) { return openAIError(res, err.statusCode || 400, err.message, 'invalid_request_error'); }

  let payload;
  try { payload = JSON.parse(rawBody || '{}'); }
  catch { return openAIError(res, 400, 'request body must be valid JSON', 'invalid_request_error'); }

  if (!payload.model) return openAIError(res, 400, 'model is required', 'invalid_request_error');
  if (config.enabledModels.length && !config.enabledModels.includes(payload.model)) {
    return openAIError(res, 400, `model is not enabled: ${payload.model}`, 'invalid_request_error');
  }

  // Track whether we mutate the payload. If we don't, we forward the raw
  // bytes as-is — no re-serialization of a potentially 100K+ token prompt.
  let mutated = false;

  // Snap reasoning_effort to a level the model actually supports. xhi/xhigh
  // map to max; values for models that can't disable reasoning are dropped.
  if (payload.reasoning_effort != null) {
    await fetchModelInfo();
    const snapped = snapReasoningLevel(payload.model, payload.reasoning_effort);
    if (snapped == null) { delete payload.reasoning_effort; mutated = true; }
    else if (snapped !== payload.reasoning_effort) { payload.reasoning_effort = snapped; mutated = true; }
  }
  const controller = new AbortController();
  let closed = res.destroyed;
  // Telemetry status: done | aborted | error. Set aborted on client close /
  // timeout, error in catch; defaults to done for normal completion.
  let sessionStatus = 'done';
  const onClose = () => { closed = true; sessionStatus = 'aborted'; controller.abort(); };
  if (closed) { sessionStatus = 'aborted'; controller.abort(); }
  else res.on('close', onClose);

  // For streaming requests, send 200 + SSE headers BEFORE acquiring the
  // throttle slot. This lets omp's fetch resolve, starts its SSE idle
  // watchdog, and lets us emit comment-frame keepalives while queued.
  // omp's iterateWithIdleTimeout counts SSE events; comment frames reset it.
  // Non-streaming requests wait normally (omp's timeout:false covers them).
  const isStream = !!payload.stream;
  let headersSentEarly = false;
  if (isStream && !closed) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    headersSentEarly = true;
  }

  try {
    await acquireThrottleSlot(res, controller.signal, {
      keepalive: headersSentEarly ? () => {
        if (!closed && !res.destroyed) {
          try { res.write(': queued\n\n'); } catch {}
        }
      } : undefined,
    });
  } catch (err) {
    if (closed) return;
    if (headersSentEarly) {
      // Already sent 200; emit an SSE error event and end the stream.
      try { res.write(`data: ${JSON.stringify({ error: { message: err.message, type: 'rate_limit_error' } })}\n\n`); } catch {}
      res.end();
    } else {
      return openAIError(res, 503, err.message, 'rate_limit_error');
    }
    return;
  }
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; sessionStatus = 'aborted'; controller.abort(); }, config.requestTimeout);
  // asynchronously (setImmediate drain) and NEVER mutates what the client
  // receives. Created here so it covers the upstream fetch + streaming.
  // Coalesce: resolve the logical session from the message prefix before
  // creating the per-request session. Runs at arrival — never in the stream.
  const { groupKey, prefixChain } = resolveGroupKey(payload.model, payload.messages || []);
  const session = createSession({ model: payload.model, stream: payload.stream, groupKey });
  const tap = new ChatTap(session, { stream: payload.stream, onFinalize: (s) => {
    if (sessionStatus === 'done') {
      try { storeStateKey(payload.model, payload.messages || [], s.responseContent, s.groupKey, prefixChain); } catch {}
    }
    // Release the accumulated response text — it was only needed for the
    // coalescing stateKey. Prevents holding large responses in memory for
    // the 5s session retention window.
    s.responseContent = '';
  }});
  broadcastEvent('session', { type: 'start', id: session.id, active: activeRequests, queued: queuedRequests });
  scheduleSessionsBroadcast();

  try {
    // Inject stream_options.include_usage to get exact token counts on the
    // final stream chunk. Only re-serialize if this actually changes the
    // payload — if the client already set it, forward raw bytes.
    if (payload.stream && !payload.stream_options?.include_usage) {
      payload.stream_options = { ...(payload.stream_options || {}), include_usage: true };
      mutated = true;
    }
    // Forward the raw bytes unless we mutated the payload — avoids a full
    // JSON.stringify of a potentially huge body on the hot path.
    const body = mutated ? JSON.stringify(payload) : rawBody;
    const upstream = await fetch(upstreamURL('/chat/completions'), {
      method: 'POST',
      headers: authHeaders({ 'Content-Type': 'application/json', Accept: payload.stream ? 'text/event-stream' : 'application/json' }),
      body,
      signal: controller.signal,
    });
    refreshUsageSoon();

    if (headersSentEarly) {
      // We already sent 200 + SSE headers. If upstream returned an error,
      // emit it as an SSE error event (can't change the HTTP status now).
      if (!upstream.ok) {
        const errText = await upstream.text().catch(() => upstream.statusText);
        try { res.write(`data: ${JSON.stringify({ error: { message: `upstream ${upstream.status}: ${errText.slice(0, 500)}`, type: 'upstream_error' } })}\n\n`); } catch {}
        res.end();
        sessionStatus = 'error';
        return;
      }
    } else {
      res.writeHead(upstream.status, safeHeaders(upstream.headers));
    }
    if (!upstream.body) return res.end();

    const reader = upstream.body.getReader();
    try {
      while (!closed) {
        const { done, value } = await reader.read();
        if (done) break;
        // Feed the tap BEFORE writeChunk. onChunk is O(1): it pushes the
        // buffer onto a queue and schedules an async drain; it does NOT
        // parse here, so it never delays bytes reaching the client.
        tap.onChunk(value);
        await writeChunk(res, value, controller.signal);
      }
    } finally {
      // Always release the upstream reader: on abort/timeout we cancel
      // promptly, and on stream errors (writeChunk reject) the body would
      // otherwise linger until upstream GC/timeout.
      await reader.cancel().catch(() => {});
    }
    if (!closed) res.end();
  } catch (err) {
    if (!closed) logError('Chat proxy failed', err);
    if (!res.headersSent && !headersSentEarly && !closed) openAIError(res, timedOut ? 504 : 502, timedOut ? 'upstream request timed out' : err.message);
    else if (!closed) {
      if (headersSentEarly) {
        try { res.write(`data: ${JSON.stringify({ error: { message: timedOut ? 'upstream request timed out' : err.message, type: 'server_error' } })}\n\n`); } catch {}
      }
      res.end();
    }
    sessionStatus = closed || timedOut ? 'aborted' : 'error';
  }
  finally {
    clearTimeout(timeout);
    res.off('close', onClose);
    releaseThrottleSlot();
    // Finalize telemetry. onEnd() is async-safe: if a drain is in flight,
    // _finish runs when caught up; otherwise it runs now. Never throws.
    try { tap.onEnd(); } catch {}
    // Coalescing is handled by the onFinalize callback inside _finish,
    // which runs after responseContent is fully populated (may be deferred
    // by an in-flight drain). This is why storeStateKey is not here.
    finalizeSession(session, sessionStatus);
  }
}

async function handleConfig(req, res) {
  if (req.method === 'GET') {
    return writeJSON(res, 200, {
      listenAddr: config.listenAddr,
      upstreamBaseURL: config.upstreamBaseURL,
      hasApiKey: !!config.apiKey,
      enabledModels: config.enabledModels,
      requestTimeout: config.requestTimeoutRaw,
      overrideConcurrency: config.overrideConcurrency,
      proxyAuthEnabled: config.proxyApiKeys.length > 0,
    });
  }

  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');

  let next;
  let apiKey = config.apiKey;
  let fileApiKey = config.fileApiKey;
  let enabledModels = config.enabledModels;
  let requestTimeout = config.requestTimeout;
  let requestTimeoutRaw = config.requestTimeoutRaw;
  let overrideConcurrency = config.overrideConcurrency;
  try {
    next = JSON.parse(await readBody(req) || '{}');
    if (typeof next.apiKey === 'string' && next.apiKey.trim()) {
      apiKey = next.apiKey.trim();
      fileApiKey = apiKey;
    }
    if (Array.isArray(next.enabledModels)) enabledModels = next.enabledModels.map((x) => String(x).trim()).filter(Boolean);
    if (typeof next.requestTimeout === 'string' && next.requestTimeout.trim()) {
      requestTimeoutRaw = next.requestTimeout.trim();
      requestTimeout = parseDuration(requestTimeoutRaw);
    }
    if (next.overrideConcurrency !== undefined) overrideConcurrency = Math.max(0, Number(next.overrideConcurrency) || 0);
  } catch (err) {
    return openAIError(res, err.statusCode || 400, err.message, 'invalid_request_error');
  }

  const nextConfig = { ...config, apiKey, fileApiKey, enabledModels, requestTimeout, requestTimeoutRaw, overrideConcurrency };
  saveConfig(nextConfig);
  config = nextConfig;
  usageCache = { data: null, time: 0 };
  concurrencyCache = { concurrent: null, limit: null, softLimit: null, user_id: null, time: 0 };
  writeJSON(res, 200, { ok: true });
}

async function handleUsage(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  const force = new URL(req.url, 'http://localhost').searchParams.get('force') === '1';
  writeJSON(res, 200, await fetchUmansUsage({ force }));
}

async function handleConcurrency(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  await fetchUmansUsage();
  const effective = getEffectiveConcurrency();
  writeJSON(res, 200, { ...effective, active: activeRequests, queued: queuedRequests });
}

async function handleSessions(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, getSessionsSnapshot());
}

// Graceful shutdown trigger from the dashboard. Responds first, then shuts
// down on next tick so the HTTP response flushes before the server closes.
function handleShutdown(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, { ok: true, message: 'shutting down' });
  setImmediate(shutdown);
}

// Restart: spawn a detached successor that survives this process's exit,
// then shut down. stdio:'ignore' + detached + unref() detaches the child on
// both Windows and POSIX so it isn't killed when we exit.
function handleRestart(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  let child;
  try {
    child = spawn(process.execPath, [__filename], {
      cwd: __dirname, detached: true, stdio: 'ignore', shell: false,
    });
    child.unref();
  } catch (err) {
    return openAIError(res, 500, `failed to spawn successor: ${err.message}`);
  }
  // Give the successor a moment to bind before we release the port. On
  // EADDRINUSE it will exit; the user sees it in the log. There's no
  // reliable cross-platform signal back, so we just shut down after a beat.
  writeJSON(res, 200, { ok: true, message: 'restarting' });
  setTimeout(shutdown, 300);
}

// Clear state: reset live session tracking and caches without restarting.
function handleClearState(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  sessions.clear();
  sessionSeq = 0;
  tpsBuckets.length = 0;
  tpsBucketsByModel.clear();
  modelCharRatio.clear();
  messageHashCache.clear();
  stateMap.clear();
  groupSummaries.clear();
  seenModels.clear();
  usageCache = { data: null, time: 0 };
  concurrencyCache = { concurrent: null, limit: null, softLimit: null, user_id: null, time: 0 };
  modelInfoCache = { data: null, time: 0 };
  if (sessionsBroadcastTimer) { clearInterval(sessionsBroadcastTimer); sessionsBroadcastTimer = null; }
  broadcastThrottled = false;
  // Broadcast the empty state directly — don't call scheduleSessionsBroadcast,
  // which would defer via setTimeout and could race with a prior pending call.
  broadcastEvent('sessions', getSessionsSnapshot());
  writeJSON(res, 200, { ok: true });
}

// System info for the admin panel.
function handleSystemInfo(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, {
    pid: process.pid,
    startedAt: new Date(startedAt).toISOString(),
    uptimeMs: Date.now() - startedAt,
    listenAddr: config.listenAddr,
    upstream: config.upstreamBaseURL,
    nodeVersion: process.version,
    activeRequests,
    queuedRequests,
    sessionsTracked: sessions.size,
  });
}
function handleEvents(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write(`event: connected\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
  // Push current sessions so a newly connected dashboard has data immediately.
  res.write(`event: sessions\ndata: ${JSON.stringify(getSessionsSnapshot())}\n\n`);
  sseClients.add(res);
  req.on('close', () => { sseClients.delete(res); });
}
async function handleRequest(req, res) {
  const url = new URL(req.url, 'http://localhost');
  if (requiresProxyAuth(url.pathname) && !authorized(req, url)) return openAIError(res, 401, 'invalid proxy api key', 'authentication_error');
  if ((url.pathname === '/' || url.pathname === '/dashboard') && req.method === 'GET') {
    const file = path.join(__dirname, 'dashboard.html');
    return writeText(res, 200, fs.readFileSync(file, 'utf8'), 'text/html; charset=utf-8');
  }
  if (url.pathname === '/health') return writeJSON(res, 200, { ok: true, upstream: config.upstreamBaseURL, hasApiKey: !!config.apiKey, proxyAuthEnabled: config.proxyApiKeys.length > 0 });
  if (url.pathname === '/api/events') return handleEvents(req, res);
  if (url.pathname === '/api/umans/sessions') return handleSessions(req, res);
  if (url.pathname === '/api/umans/usage') return handleUsage(req, res);
  if (url.pathname === '/api/umans/concurrency') return handleConcurrency(req, res);
  if (url.pathname === '/v1/models/info') return handleModelsInfo(req, res);
  if (url.pathname === '/v1/models') return handleModels(req, res);
  if (url.pathname.startsWith('/v1/models/')) return handleModels(req, res, decodeURIComponent(url.pathname.slice('/v1/models/'.length)));
  if (url.pathname === '/v1/chat/completions') return handleChat(req, res);
  if (url.pathname.startsWith('/v1/')) return openAIError(res, 404, `unsupported endpoint: ${url.pathname}`, 'invalid_request_error');
  if (url.pathname === '/api/shutdown') return handleShutdown(req, res);
  if (url.pathname === '/api/restart') return handleRestart(req, res);
  if (url.pathname === '/api/clear-state') return handleClearState(req, res);
  if (url.pathname === '/api/debug/coalesce') return writeJSON(res, 200, coalesceDebug);
  if (url.pathname === '/api/system/info') return handleSystemInfo(req, res);
  writeText(res, 404, 'Not Found');
}

let host, port;
try {
  ({ host, port } = parseListenAddr(config.listenAddr));
} catch (err) {
  console.error(`Startup error: ${err.message}`);
  process.exit(1);
}
const server = http.createServer((req, res) => {
  handleRequest(req, res).catch((err) => {
    logError('Request handler failed', err);
    if (!res.headersSent) openAIError(res, 500, err.message);
    else res.end();
  });
});

server.on('error', (err) => {
  // EADDRINUSE during a restart: the predecessor is still releasing the
  // port. Retry with backoff instead of dying. Other errors are fatal.
  if (err.code === 'EADDRINUSE') {
    server.listenRetries = (server.listenRetries || 0) + 1;
    if (server.listenRetries > 40) { console.error('Giving up: port still in use after 20s'); process.exit(1); }
    console.error(`Port ${port} in use; successor retrying (attempt ${server.listenRetries})…`);
    setTimeout(() => {
      server.close(() => {});
      server.listen({ port, host, exclusive: true });
    }, 500);
    return;
  }
  console.error(`Server error: ${err.message}`);
  process.exit(1);
});

function shutdown() {
  console.log('Shutting down UMANS Proxy');
  for (const res of sseClients) res.end();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);

try {
  server.listen({ port, host, exclusive: true }, () => {
    console.log(`UMANS Proxy listening on http://${host}:${port}`);
    console.log(`Upstream: ${config.upstreamBaseURL}`);
    console.log(`API key: ${config.apiKey ? 'configured' : 'missing'}`);
  });
} catch (err) {
  console.error(`Startup error: ${err.message}`);
  process.exit(1);
}
