'use strict';

const fs = require('fs');
const path = require('path');
const state = require('./state');
const { authHeaders, upstreamURL } = require('./auth');
const { logError } = require('./http');
const { broadcastEvent } = require('./sessions');
const { fetchCapHealth, decide429Backoff } = require('./cap-health');

// Defensive: throttleWaiters was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!Array.isArray(state.throttleWaiters)) state.throttleWaiters = [];
if (typeof state.burstDisabledUntil !== 'number') state.burstDisabledUntil = 0;
if (typeof state.usageEverFetched !== 'boolean') state.usageEverFetched = false;
if (!Array.isArray(state.releaseCooldowns)) state.releaseCooldowns = [];
if (!Array.isArray(state.phantomSamples)) state.phantomSamples = [];
state.cooldownWakeTimer = state.cooldownWakeTimer ?? null;

// Burst cooldown after an upstream 429. While active, the effective
// concurrency limit is clamped to the soft limit (no bursting). Duration is
// 24h / 20 = 1.2h (72 minutes). The epoch is persisted to .runtime/ so a
// process restart does not let bursting back on early (and re-hit the limit).
const BURST_COOLDOWN_MS = (24 * 60 * 60 * 1000) / 20;
const BURST_COOLDOWN_FILE = path.join(__dirname, '..', '.runtime', 'burst-cooldown.json');

// Cold-start admission floor: before the first successful /usage fetch, a null
// concurrency limit is ignorance, not "unlimited". Admit at most this many
// concurrent requests so a /usage outage can't make the proxy an unthrottled
// relay (account-wide 429/ban risk).
const COLD_START_FLOOR = 1;

// If /usage succeeds but reports no positive max concurrency (missing/0/null),
// treat that as a broken usage payload and hard-bound admissions to 4. This is
// intentionally separate from COLD_START_FLOOR: once the proxy is warm, allow a
// small amount of useful work, but never fail open.
const BROKEN_USAGE_LIMIT = 4;

// Release cooldown (ported from sluice's release_cooldown): a freed permit
// rests this long before it is reusable. Blunts the lag race where umans
// hasn't decremented its concurrent_sessions counter yet — the prior local
// design patched this by manually decaying the cached upstream figure, which
// would corrupt the windowed phantom estimate. The cooldown moves that
// protection to the permit level instead.
const RELEASE_COOLDOWN_MS = 2000;

// Phantom window (ported from sluice's phantom_window, Plan 003): the number
// of /usage samples over which the sustained (observed − local) excess is
// tracked. A transient lag spike (a just-completed request still in the
// provider's lagged counter) appears in only one sample, so the windowed min
// drops it; a genuine phantom present in every sample survives and shrinks
// the gate. As phantoms age out upstream the observed count falls, the
// estimate drops, and the gate reopens.
const PHANTOM_WINDOW = 3;

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

// Fraction of burst headroom currently available. Used for local admission:
// admit the soft limit plus the available portion of burst headroom, up to the
// upstream hard cap. The prior static hard-cap-minus-one guardrail was removed:
// the dynamic controller (phantom absorption, burst cooldown, priority box)
// governs the margin now, so the full reported burst ceiling is usable.
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
  if (!soft) return null;
  if (!hard || hard <= soft) return hard && hard < soft ? hard : soft;
  const burstCeiling = hard;
  return Math.floor(soft + (burstCeiling - soft) * burstQuota(concurrency));
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
  const rawQuotaLimit = concurrencyQuotaLimit(concurrency);
  const quotaLimit = rawQuotaLimit ?? BROKEN_USAGE_LIMIT;
  const boxMs = boxedUntilMs(data);
  const applied = applyOverride(quotaLimit, soft, state.config.overrideConcurrency || 0);
  let limit = clampBoxedLimit(applied.limit, applied.softLimit, boxMs);
  if (burstCooldownActive() && applied.softLimit != null && limit != null) limit = Math.min(limit, applied.softLimit);
  // Phantom absorption mirrors getEffectiveConcurrency: shrink the pool by the
  // windowed sustained upstream-over-local excess. extractThrottle runs right
  // after a fresh fetch (the sample set is current), so the estimate is live.
  if (limit != null) {
    const phantom = currentPhantomEstimate();
    if (phantom > 0) limit = Math.max(0, limit - phantom);
  }
  return {
    concurrent, soft, hard, softLimit: applied.softLimit, limit, quotaLimit, overridden: applied.overridden,
    burstQuota: burstQuota(concurrency), boxedUntil: boxMs ? new Date(boxMs).toISOString() : null, boxed: boxedActive(boxMs),
    burstCooldown: burstCooldownActive(), burstCooldownUntil: state.burstDisabledUntil > 0 ? new Date(state.burstDisabledUntil).toISOString() : null,
    coolingDown: coolingDownNow(), phantomEstimate: currentPhantomEstimate(),
    active: state.activeRequests, queued: state.queuedRequests,
  };
}

let usageFetchPromise = null; // in-flight /usage fetch dedup for direct callers

async function fetchUmansUsage({ force = false } = {}) {
  if (!state.config.apiKey) return { ok: false, error: 'UMANS API key is not configured', usage: null, limits: null, throttle: extractThrottle(null) };
  if (!force && state.usageCache.data && Date.now() - state.usageCache.time < state.USAGE_TTL_MS) {
    // Recompute live throttle fields (burst cooldown, priority box, active,
    // queued) so a cached serve never reports a stale cooldown state.
    state.usageCache.data.throttle = extractThrottle(state.usageCache.data.raw);
    return state.usageCache.data;
  }
  // Dedup: concurrent direct callers (handleUsage/handleConcurrency/cold-acquire)
  // share one in-flight /usage request instead of firing a storm of identical
  // fetches against the upstream.
  if (usageFetchPromise) return usageFetchPromise;
  usageFetchPromise = doFetchUmansUsage();
  try {
    return await usageFetchPromise;
  } finally {
    usageFetchPromise = null;
  }
}

async function doFetchUmansUsage() {
  try {
    // Capture locally-held permits BEFORE the await (sluice WI-017): the
    // provider's concurrent_sessions reflects the moment it served the
    // request, not the moment we read activeRequests after the network I/O.
    // Permits can be acquired/released during the fetch; pairing a mismatched
    // (observed, local) would corrupt the windowed phantom estimate.
    const heldAtFetch = state.activeRequests;
    const resp = await fetch(upstreamURL('/usage'), {
      headers: authHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) throw new Error(`UMANS /usage returned ${resp.status}`);
    const raw = await resp.json();
    const concurrent = Number(raw?.usage?.concurrent_sessions ?? raw?.usage?.concurrent ?? raw?.concurrent_sessions ?? 0) || 0;
    // Record the (observed, local) pair for windowed phantom estimation,
    // aligned to the moment the provider counted the sessions (sluice Plan
    // 003). Bounded to PHANTOM_WINDOW samples; oldest drops off the front.
    if (state.phantomSamples.length >= PHANTOM_WINDOW) state.phantomSamples.shift();
    state.phantomSamples.push({ observed: concurrent, local: heldAtFetch });
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
      concurrent,
      limit: concurrencyQuotaLimit(raw?.limits?.concurrency || {}) ?? BROKEN_USAGE_LIMIT,
      softLimit: firstNumber(raw?.limits?.concurrency?.limit),
      boxedUntil: boxedUntilMs(raw),
      time: Date.now(),
    };
    state.usageEverFetched = true;
    wakeThrottleWaiters();
    broadcastEvent('usage', result);
    return result;
  } catch (err) {
    logError('UMANS /usage fetch failed', err);
    // A failed refresh can still change admission: once the cache is stale,
    // getEffectiveConcurrency collapses stale upstream concurrency to our
    // local activeRequests. Wake waiters so they can re-check that state.
    wakeThrottleWaiters();
    return { ok: false, error: err.message, usage: null, limits: null, throttle: extractThrottle(null) };
  }
}

function refreshUsageSoon() {
  if (state.refreshUsageInFlight) return;
  if (state.refreshUsageTimer) return;
  if (state.usageCache.data && Date.now() - state.usageCache.time < state.REFRESH_USAGE_MIN_MS) return;
  state.refreshUsageInFlight = true;
  state.refreshUsageTimer = setTimeout(() => { state.refreshUsageTimer = null; }, state.REFRESH_USAGE_MIN_MS).unref();
  fetchUmansUsage({ force: true })
    .catch(() => {})
    .finally(() => { state.refreshUsageInFlight = false; });
}

function burstCooldownActive() {
  return state.burstDisabledUntil > 0 && Date.now() < state.burstDisabledUntil;
}

// Count permits currently in release cooldown (freed but still resting). The
// deque is pushed in release order, which is monotonic in time, so the head
// is always the earliest-expiring entry — prune from the front.
function coolingDownNow() {
  const now = Date.now();
  const cd = state.releaseCooldowns;
  while (cd.length && cd[0] <= now) cd.shift();
  return cd.length;
}

// Windowed phantom estimate (sluice Plan 003 port): the sustained excess of
// upstream-observed concurrent_sessions over locally-held permits, taken as
// the min over the last PHANTOM_WINDOW samples. A transient lag spike appears
// in one sample only and is dropped by the min; a phantom present in every
// sample survives. Returns 0 when no samples have been recorded yet.
function currentPhantomEstimate() {
  const samples = state.phantomSamples;
  if (!samples || samples.length === 0) return 0;
  let minExcess = Infinity;
  for (let i = 0; i < samples.length; i++) {
    const excess = samples[i].observed - samples[i].local;
    if (excess < minExcess) minExcess = excess;
  }
  return Math.max(0, minExcess);
}

// Schedule a one-shot wake for when the earliest cooling permit expires, so a
// freed-but-resting slot doesn't strand a queued waiter until the next /usage
// refresh. Idempotent — a pending wake is reused. The timer is unref'd so it
// never keeps the process alive on its own.
function scheduleCooldownWake() {
  if (state.cooldownWakeTimer) return;
  if (state.releaseCooldowns.length === 0) return;
  state.cooldownWakeTimer = setTimeout(() => {
    state.cooldownWakeTimer = null;
    wakeThrottleWaiters();
  }, RELEASE_COOLDOWN_MS + 1).unref();
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
  if (effectiveLimit == null && state.usageEverFetched) effectiveLimit = BROKEN_USAGE_LIMIT;
  if ((burstCooldownActive() || boxedActive(state.concurrencyCache.boxedUntil)) && softLimit && effectiveLimit != null) {
    effectiveLimit = Math.min(effectiveLimit, softLimit);
  }
  // Phantom absorption (sluice port): shrink the effective permit pool by the
  // windowed sustained excess of upstream-observed over locally-held sessions.
  // This lets the gate absorb phantoms we didn't create (and external clients
  // on the same key) and reopen as the provider's count falls back. Applied
  // AFTER the box/burst-cooldown clamp to soft, so phantoms can tighten below
  // soft but the burst ceiling still holds. Only applied while the usage
  // reading is fresh — a stale reading must not shrink the gate on outdated
  // sample data (fail-safe: stale → don't trust the estimate, don't starve).
  const cacheStale = Date.now() - state.concurrencyCache.time > state.USAGE_TTL_MS;
  if (effectiveLimit != null && !cacheStale) {
    const phantom = currentPhantomEstimate();
    if (phantom > 0) effectiveLimit = Math.max(0, effectiveLimit - phantom);
  }
  // C3: while the usage cache is stale, the cached upstream concurrent_sessions
  // is stale-high. Prefer our own activeRequests for the DISPLAY field so the
  // dashboard doesn't report a figure that no longer reflects reality. (The
  // admission decision above already skipped phantom absorption when stale.)
  let concurrent = state.concurrencyCache.concurrent || 0;
  if (cacheStale && concurrent > state.activeRequests) concurrent = state.activeRequests;
  return { concurrent, limit: effectiveLimit, softLimit, overridden, boxed: boxedActive(state.concurrencyCache.boxedUntil), boxedUntil: state.concurrencyCache.boxedUntil ? new Date(state.concurrencyCache.boxedUntil).toISOString() : null, burstCooldown: burstCooldownActive(), burstCooldownUntil: state.burstDisabledUntil > 0 ? new Date(state.burstDisabledUntil).toISOString() : null, coolingDown: coolingDownNow(), phantomEstimate: cacheStale ? 0 : currentPhantomEstimate() };
}

// Arm the burst cooldown after an upstream 429. Each (genuine) 429 (re)arms
// the full cooldown window from now, so repeated cap hits keep bursting off.
// We do NOT wake queued waiters here: waking them under the freshly-clamped
// limit lets them immediately hit the (still-429'ing) upstream and slide the
// window again (cascade). Let in-flight requests drain; their releases wake
// the queue as real slots free (C5).
function armBurstCooldown() {
  const now = Date.now();
  state.burstDisabledUntil = now + BURST_COOLDOWN_MS;
  persistBurstCooldown();
  logError(`Upstream 429: burst disabled for ${BURST_COOLDOWN_MS / 60000}m`);
  broadcastEvent('burst-cooldown', { disabledUntil: state.burstDisabledUntil });
}

// Entry point invoked from chat.js on an upstream 429. If a session cookie is
// configured, the 429 backoff is gated on cap-health: we fetch cap-health
// fresh and only arm the cooldown if `blocksToday` incremented since the last
// fetch — a real cap block, not a transient per-minute rate limit. Without a
// cookie cap-health is unreachable, so we fall back to arming unconditionally
// (the legacy fail-safe behavior). The gate is fire-and-forget: chat.js does
// not await it, and the request path is never blocked on an app.umans.ai
// round-trip.
function notifyUpstream429() {
  if (state.config.sessionCookie) {
    gate429OnCapHealth();
    return;
  }
  armBurstCooldown();
}

async function gate429OnCapHealth() {
  try {
    const prev = state.lastBlocksToday;
    const health = await fetchCapHealth({ force: true });
    const current = health?.blocksToday ?? null;
    if (decide429Backoff(prev, current)) {
      armBurstCooldown();
    } else {
      logError(`Upstream 429: blocksToday unchanged (${prev}→${current}); burst not disabled`);
    }
  } catch (err) {
    logError('cap-health 429 gate failed; arming backoff', err);
    armBurstCooldown();
  }
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
  if (limit == null) {
    // Upstream reports no concurrency cap. But if we've never successfully
    // fetched /usage (cold boot or sustained outage), null is ignorance, not
    // "unlimited" — admit only the cold-start floor so a down /usage can't
    // turn the proxy into an unthrottled relay (C2).
    if (!state.usageEverFetched) return state.activeRequests < COLD_START_FLOOR;
    return true;
  }
  // Resizeable-pool admission (sluice port): the limit already has phantom
  // absorption baked in (getEffectiveConcurrency shrinks it by the windowed
  // sustained upstream-over-local excess), so admission is purely local —
  // held permits plus cooling (freed-but-resting) permits versus that pool.
  // The prior max(local, upstream-concurrent) rule is gone: the upstream
  // figure now drives the pool size via the phantom estimate, not a per-
  // request comparison, so a sustained phantom shrinks the gate and reopens
  // as it clears instead of holding it closed until the provider decrements.
  const held = state.activeRequests;
  const cooling = coolingDownNow();
  if (held + cooling >= limit) return false;
  const soft = effective.softLimit;
  if (soft && held + cooling >= soft && state.queuedRequests > 0) return false;
  return true;
}

async function acquireThrottleSlot(res, signal) {
  if (signal?.aborted) throw new Error('aborted');
  let effective = getEffectiveConcurrency();
  if (effective.limit === null) {
    if (state.activeRequests === 0) refreshUsageSoon();
    else { await fetchUmansUsage(); effective = getEffectiveConcurrency(); }
  }
  let woken = false;
  while (!canStart(effective)) {
    if (signal?.aborted) throw new Error('aborted');
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
        // Schedule a wake for the earliest cooling-permit expiry so this
        // queued waiter is claimed promptly, not stranded until the next
        // /usage refresh. Runs synchronously inside the executor, before the
        // await suspends, so the timer is armed while we wait.
        if (coolingDownNow() > 0) scheduleCooldownWake();
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
  // Release cooldown (sluice port): the freed permit rests RELEASE_COOLDOWN_MS
  // before it is reusable, blunting the lag race where umans hasn't
  // decremented its concurrent_sessions counter yet. The prior local design
  // patched this by manually decaying the cached upstream concurrent figure
  // on each release — that decay would corrupt the windowed phantom estimate
  // (it mutates the observed count the estimate keys on). The cooldown moves
  // that protection to the permit level instead, leaving the usage cache as
  // the provider's authoritative reading. coolingDownNow() prunes expired
  // entries; the deque is pushed in release order (monotonic, stays sorted).
  state.releaseCooldowns.push(Date.now() + RELEASE_COOLDOWN_MS);
  coolingDownNow();
  // Try to wake a queued waiter now (in case the gate has room without the
  // cooling permit), and arm the cooldown-wake timer so a waiter is claimed
  // the moment this resting permit expires.
  wakeThrottleWaiters();
  scheduleCooldownWake();
  refreshUsageSoon();
  broadcastEvent('session', { type: 'end', active: state.activeRequests, queued: state.queuedRequests });
}

// Start a background refresh cycle that keeps the usage cache warm between
// requests. Runs on a timer at the refresh cadence (REFRESH_USAGE_MIN_MS), so
// throttle decisions always have fresh data without blocking the request path.
// Safe to call multiple times — the timer guard prevents duplicates.
function startUsageBackgroundRefresh() {
  if (state.usageBackgroundTimer) return;
  const tick = () => {
    if (state.config?.apiKey) refreshUsageSoon();
    state.usageBackgroundTimer = setTimeout(tick, Math.max(state.REFRESH_USAGE_MIN_MS, 1000)).unref();
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
  RELEASE_COOLDOWN_MS,
  PHANTOM_WINDOW,
  coolingDownNow,
  currentPhantomEstimate,
  wakeThrottleWaiters,
};
