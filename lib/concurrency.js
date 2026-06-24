'use strict';

const state = require('./state');
const { authHeaders, upstreamURL } = require('./auth');
const { logError } = require('./http');
const { broadcastEvent } = require('./sessions');

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
  const { limit, softLimit, overridden } = applyOverride(quotaLimit, soft, state.config.overrideConcurrency || 0);
  return {
    concurrent, soft, hard, softLimit, limit, quotaLimit, overridden,
    burstQuota: burstQuota(concurrency),
    active: state.activeRequests, queued: state.queuedRequests,
  };
}

async function fetchUmansUsage({ force = false } = {}) {
  if (!state.config.apiKey) return { ok: false, error: 'UMANS API key is not configured', usage: null, limits: null, throttle: extractThrottle(null) };
  if (!force && state.usageCache.data && Date.now() - state.usageCache.time < state.USAGE_TTL_MS) return state.usageCache.data;
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
      time: Date.now(),
    };
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

function getEffectiveConcurrency() {
  const { limit, softLimit, overridden } = applyOverride(
    state.concurrencyCache.limit,
    state.concurrencyCache.softLimit,
    state.config.overrideConcurrency || 0,
  );
  return { concurrent: state.concurrencyCache.concurrent || 0, limit, softLimit, overridden };
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
  let keepaliveTimer = null;
  try {
    while (!canStart(effective)) {
      if (signal?.aborted) throw new Error('aborted');
      if (!keepaliveTimer && keepalive) keepaliveTimer = setInterval(keepalive, 3000);
      state.queuedRequests++;
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
        state.queuedRequests--;
      }
      effective = getEffectiveConcurrency();
    }
  } finally {
    clearInterval(keepaliveTimer);
  }
  if (signal?.aborted) throw new Error('aborted');
  state.activeRequests++;
  refreshUsageSoon();
  broadcastEvent('session', { type: 'start', active: state.activeRequests, queued: state.queuedRequests });
}

function releaseThrottleSlot() {
  if (state.activeRequests > 0) state.activeRequests--;
  refreshUsageSoon();
  broadcastEvent('session', { type: 'end', active: state.activeRequests, queued: state.queuedRequests });
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
};
