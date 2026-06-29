'use strict';

const fs = require('fs');
const path = require('path');
const state = require('./state');
const { authHeaders, upstreamURL } = require('./auth');
const { logError } = require('./http');
const { broadcastEvent } = require('./sessions');

// Defensive: throttleWaiters was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!Array.isArray(state.throttleWaiters)) state.throttleWaiters = [];
if (typeof state.burstDisabledUntil !== 'number') state.burstDisabledUntil = 0;

// Burst cooldown after an upstream 429. While active, the effective
// concurrency limit is clamped to the soft limit (no bursting). Duration is
// 24h / 20 = 1.2h (72 minutes). The epoch is persisted to .runtime/ so a
// process restart does not let bursting back on early (and re-hit the limit).
const BURST_COOLDOWN_MS = (24 * 60 * 60 * 1000) / 20;
const BURST_COOLDOWN_FILE = path.join(__dirname, '..', '.runtime', 'burst-cooldown.json');

// Load the persisted cooldown epoch at module load. state.js survives hot
// reload but NOT a full process restart — this file is the durable copy.
function loadBurstCooldown() {
  try {
    const raw = JSON.parse(fs.readFileSync(BURST_COOLDOWN_FILE, 'utf8'));
    const until = Number(raw.burstDisabledUntil);
    if (Number.isFinite(until) && until > 0) state.burstDisabledUntil = until;
  } catch {}
}
function persistBurstCooldown() {
  try {
    fs.mkdirSync(path.dirname(BURST_COOLDOWN_FILE), { recursive: true });
    fs.writeFileSync(BURST_COOLDOWN_FILE, JSON.stringify({ burstDisabledUntil: state.burstDisabledUntil }) + '\n');
  } catch (err) { logError('Failed to persist burst cooldown', err); }
}
loadBurstCooldown();
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
    if (value == null) continue;
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) continue;
    return Math.max(0, Math.min(1, n > 1 ? n / 100 : n));
  }
  return null;
}

// Fraction of burst headroom currently available: 0 = burst exhausted
// (effective limit collapses to soft), 1 = full burst (effective = hard).
// Reads ONLY live "remaining/available burst" fields. burst_pct and
// burst_percent are deliberately excluded — those define the hard cap
// (hard = soft * (1 + burst_pct)) in concurrencyHardLimit, not the live
// remaining quota. When no live indicator is reported, assume full burst
// is available; retreat to soft is handled by the upstream-429 cooldown,
// not here.
function burstQuota(concurrency) {
  return percentValue(
    concurrency.burst_remaining_pct,
    concurrency.burst_pct_remaining,
    concurrency.remaining_burst_pct,
    concurrency.burst_available_pct,
    concurrency.burst_remaining_percent,
    concurrency.remaining_burst_percent,
    concurrency.burst_percent_remaining,
  ) ?? 1;
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

function boxedUntilMs(data) {
  const raw = data?.usage?.priority?.boxed_until ?? data?.priority?.boxed_until ?? data?.boxed_until;
  if (!raw) return null;
  const ms = new Date(raw).getTime();
  return Number.isFinite(ms) && ms > 0 ? ms : null;
}

function boxedActive(dataOrMs) {
  const ms = typeof dataOrMs === 'number' ? dataOrMs : boxedUntilMs(dataOrMs);
  return ms != null && Date.now() < ms;
}

function clampBoxedLimit(limit, softLimit, boxMs) {
  if (boxedActive(boxMs) && softLimit && limit != null) return Math.min(limit, softLimit);
  return limit;
}

function extractThrottle(data) {
  const usage = data?.usage || data || {};
  const limits = data?.limits || usage?.limits || {};
  const concurrency = limits?.concurrency || {};
  const concurrent = Number(usage.concurrent_sessions ?? usage.concurrent ?? data?.concurrent_sessions ?? 0) || 0;
  const soft = firstNumber(concurrency.limit);
  const hard = concurrencyHardLimit(concurrency);
  const quotaLimit = concurrencyQuotaLimit(concurrency);
  const boxMs = boxedUntilMs(data);
  const applied = applyOverride(quotaLimit, soft, state.config.overrideConcurrency || 0);
  let limit = clampBoxedLimit(applied.limit, applied.softLimit, boxMs);
  if (burstCooldownActive() && applied.softLimit != null && limit != null) limit = Math.min(limit, applied.softLimit);
  return {
    concurrent, soft, hard, softLimit: applied.softLimit, limit, quotaLimit, overridden: applied.overridden,
    burstQuota: burstQuota(concurrency), boxedUntil: boxMs ? new Date(boxMs).toISOString() : null, boxed: boxedActive(boxMs),
    burstCooldown: burstCooldownActive(), burstCooldownUntil: state.burstDisabledUntil > 0 ? new Date(state.burstDisabledUntil).toISOString() : null,
    active: state.activeRequests, queued: state.queuedRequests,
  };
}

async function fetchUmansUsage({ force = false } = {}) {
  if (!state.config.apiKey) return { ok: false, error: 'UMANS API key is not configured', usage: null, limits: null, throttle: extractThrottle(null) };
  if (!force && state.usageCache.data && Date.now() - state.usageCache.time < state.USAGE_TTL_MS) {
    // Recompute live throttle fields (burst cooldown, priority box, active,
    // queued) so a cached serve never reports a stale cooldown state.
    state.usageCache.data.throttle = extractThrottle(state.usageCache.data.raw);
    return state.usageCache.data;
  }
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
    state.usageCache = { data: result, time: Date.now() };
    state.concurrencyCache = {
      concurrent: Number(raw?.usage?.concurrent_sessions ?? 0) || 0,
      limit: concurrencyQuotaLimit(raw?.limits?.concurrency || {}),
      softLimit: firstNumber(raw?.limits?.concurrency?.limit),
      boxedUntil: boxedUntilMs(raw),
      time: Date.now(),
    };
    wakeThrottleWaiters();
    broadcastEvent('usage', result);
    return result;
  } catch (err) {
    logError('UMANS /usage fetch failed', err);
    return { ok: false, error: err.message, usage: null, limits: null, throttle: extractThrottle(null) };
  }
}

function refreshUsageSoon() {
  if (state.refreshUsageInFlight) return;
  if (state.refreshUsageTimer) return;
  if (state.usageCache.data && Date.now() - state.usageCache.time < state.REFRESH_USAGE_MIN_MS) return;
  state.refreshUsageInFlight = true;
  state.refreshUsageTimer = setTimeout(() => { state.refreshUsageTimer = null; }, state.REFRESH_USAGE_MIN_MS);
  fetchUmansUsage({ force: true })
    .catch(() => {})
    .finally(() => { state.refreshUsageInFlight = false; });
}

function burstCooldownActive() {
  return state.burstDisabledUntil > 0 && Date.now() < state.burstDisabledUntil;
}

function getEffectiveConcurrency() {
  const { limit, softLimit, overridden } = applyOverride(
    state.concurrencyCache.limit,
    state.concurrencyCache.softLimit,
    state.config.overrideConcurrency || 0,
  );
  // While burst cooldown is active, or the upstream account is in a priority
  // box (usage.priority.boxed_until in the future), clamp to the soft limit so
  // requests above the base concurrency are not admitted.
  let effectiveLimit = limit;
  if ((burstCooldownActive() || boxedActive(state.concurrencyCache.boxedUntil)) && softLimit && effectiveLimit != null) {
    effectiveLimit = Math.min(effectiveLimit, softLimit);
  }
  return { concurrent: state.concurrencyCache.concurrent || 0, limit: effectiveLimit, softLimit, overridden, boxed: boxedActive(state.concurrencyCache.boxedUntil), boxedUntil: state.concurrencyCache.boxedUntil ? new Date(state.concurrencyCache.boxedUntil).toISOString() : null, burstCooldown: burstCooldownActive(), burstCooldownUntil: state.burstDisabledUntil > 0 ? new Date(state.burstDisabledUntil).toISOString() : null };
}

// Arm the burst cooldown after an upstream 429. Each 429 (re)arms the full
// cooldown window from now, so repeated 429s keep bursting off.
function notifyUpstream429() {
  const now = Date.now();
  state.burstDisabledUntil = now + BURST_COOLDOWN_MS;
  persistBurstCooldown();
  logError(`Upstream 429: burst disabled for ${BURST_COOLDOWN_MS / 60000}m`);
  wakeThrottleWaiters();
  broadcastEvent('burst-cooldown', { disabledUntil: state.burstDisabledUntil });
}

// Clear the burst cooldown (manual override). Resets the in-memory epoch,
// persists the cleared state so a restart doesn't re-arm it, wakes throttled
// waiters, and pushes a burst-cooldown event + fresh usage so the dashboard
// drops the badge and the limit unclamps immediately.
function clearBurstCooldown() {
  state.burstDisabledUntil = 0;
  persistBurstCooldown();
  logError('Burst cooldown cleared manually');
  wakeThrottleWaiters();
  broadcastEvent('burst-cooldown', { disabledUntil: 0 });
  fetchUmansUsage({ force: true }).catch(() => {});
}
function canStart(effective) {
  const limit = effective.limit;
  if (limit == null) return true;
  const known = Math.max(state.activeRequests, effective.concurrent || 0);
  if (known >= limit) return false;
  const soft = effective.softLimit;
  if (soft && known >= soft && state.queuedRequests > 0) return false;
  return true;
}

async function acquireThrottleSlot(res, signal, { keepalive } = {}) {
  if (signal?.aborted) throw new Error('aborted');
  let effective = getEffectiveConcurrency();
  if (effective.limit === null) {
    if (state.activeRequests === 0) refreshUsageSoon();
    else { await fetchUmansUsage(); effective = getEffectiveConcurrency(); }
  }
  let woken = false;
  let keepaliveTimer = null;
  try {
    while (!canStart(effective)) {
      if (signal?.aborted) throw new Error('aborted');
      if (!keepaliveTimer && keepalive) keepaliveTimer = setInterval(keepalive, 3000);
      state.queuedRequests++;
      try {
        woken = await new Promise((resolve, reject) => {
          const waiter = { resolve: () => resolve(true), reject, signal };
          const onAbort = () => {
            const idx = state.throttleWaiters.indexOf(waiter);
            if (idx >= 0) state.throttleWaiters.splice(idx, 1);
            reject(new Error('aborted'));
          };
          waiter.onAbort = onAbort;
          state.throttleWaiters.push(waiter);
          signal?.addEventListener('abort', onAbort, { once: true });
        });
      } finally {
        state.queuedRequests--;
      }
      if (signal?.aborted) { if (woken) state.activeRequests--; throw new Error('aborted'); }
      // If woken by wakeThrottleWaiters, activeRequests was pre-incremented; an
      // abort here must release that slot or it leaks (the caller's acquire
      // catch returns without release). If not woken, nothing was incremented.
      if (woken) break;
      effective = getEffectiveConcurrency();
    }
  } finally {
    clearInterval(keepaliveTimer);
  }
  if (signal?.aborted) { if (woken) state.activeRequests--; throw new Error('aborted'); }
  if (!woken) state.activeRequests++;
  refreshUsageSoon();
}

// Wake eligible waiters after a slot frees or usage refreshes. Resolves the
// head waiter if it can now start. Pre-increments activeRequests BEFORE
// resolving so the next loop iteration's canStart() sees the updated count —
// otherwise it over-resolves waiters beyond the limit (each resolve is
// async, so activeRequests++ in acquireThrottleSlot hasn't run yet).
function wakeThrottleWaiters() {
  while (state.throttleWaiters.length > 0) {
    const effective = getEffectiveConcurrency();
    if (!canStart(effective)) break;
    const waiter = state.throttleWaiters.shift();
    if (waiter.onAbort) waiter.signal?.removeEventListener?.('abort', waiter.onAbort);
    state.activeRequests++;
    waiter.resolve();
  }
}

function releaseThrottleSlot() {
  if (state.activeRequests > 0) state.activeRequests--;
  // Decay the upstream concurrent_sessions figure by the session we just
  // released — the upstream count includes ours, so without this the stale
  // figure blocks admission for ~10s (cache TTL) after our own requests
  // drain. Clamp at activeRequests so external load (the gap concurrent -
  // activeRequests) is preserved; a fresh /usage fetch re-authoritatively
  // resets it.
  const c = state.concurrencyCache.concurrent;
  if (typeof c === 'number' && c > state.activeRequests) {
    state.concurrencyCache.concurrent = c - 1;
  }
  refreshUsageSoon();
  wakeThrottleWaiters();
  broadcastEvent('session', { type: 'end', active: state.activeRequests, queued: state.queuedRequests });
}

// Start a background refresh cycle that keeps the usage cache warm between
// requests. Runs on a timer at half the USAGE_TTL_MS interval, so throttle
// decisions always have fresh data without blocking the request path.
// Safe to call multiple times — the timer guard prevents duplicates.
function startUsageBackgroundRefresh() {
  if (state.usageBackgroundTimer) return;
  const tick = () => {
    if (state.config?.apiKey) refreshUsageSoon();
    state.usageBackgroundTimer = setTimeout(tick, Math.max(state.USAGE_TTL_MS / 2, 1000)).unref();
  };
  tick();
}


module.exports = {
  firstNumber,
  concurrencyHardLimit,
  percentValue,
  burstQuota,
  concurrencyQuotaLimit,
  applyOverride,
  extractThrottle,
  fetchUmansUsage,
  refreshUsageSoon,
  getEffectiveConcurrency,
  canStart,
  acquireThrottleSlot,
  releaseThrottleSlot,
  notifyUpstream429,
  clearBurstCooldown,
  BURST_COOLDOWN_FILE,
  startUsageBackgroundRefresh,
  burstCooldownActive,
  boxedUntilMs,
  boxedActive,
};
