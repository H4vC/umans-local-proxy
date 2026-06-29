'use strict';

// Fetches account cap/abuse health from app.umans.ai — the WEB APP host, which
// authenticates via a browser session cookie rather than the API bearer key
// used against api.code.umans.ai. This is a different host + auth scheme from
// the chat/usage/models upstream, so it lives here with its own fetch path.
//
// Two consumers:
//   1. Dashboard (admin panel): raw JSON display via GET /api/umans/cap-health.
//   2. The 429 backoff gate (lib/concurrency.js): on an upstream 429, fetch
//      cap-health fresh and only arm the burst cooldown if `blocksToday`
//      incremented since the last fetch — a real cap block, not a transient
//      per-minute rate limit.

const fs = require('fs');
const path = require('path');
const state = require('./state');
const { logError } = require('./http');

const CAP_HEALTH_URL = 'https://app.umans.ai/api/account/cap-health';
const CAP_HEALTH_TTL_MS = 30 * 1000;
const CAP_HEALTH_BASELINE_FILE = path.join(__dirname, '..', '.runtime', 'cap-health-baseline.json');

// state.js is never purged on hot reload; backfill fields added after the
// running process started so an old singleton doesn't crash this module.
if (typeof state.CAP_HEALTH_TTL_MS !== 'number') state.CAP_HEALTH_TTL_MS = CAP_HEALTH_TTL_MS;
if (!state.capHealthCache) state.capHealthCache = { data: null, time: 0 };
if (state.capHealthInFlight === undefined) state.capHealthInFlight = null;
if (state.lastBlocksToday === undefined) state.lastBlocksToday = null;

// Durable baseline for increment detection across a full process restart.
// state.js survives hot reload but NOT restart; this file is the durable copy.
function loadCapHealthBaseline() {
  try {
    const raw = JSON.parse(fs.readFileSync(CAP_HEALTH_BASELINE_FILE, 'utf8'));
    const n = Number(raw.lastBlocksToday);
    if (Number.isFinite(n) && n >= 0) state.lastBlocksToday = n;
  } catch { /* no baseline yet — first fetch will seed it */ }
}
function persistCapHealthBaseline() {
  try {
    fs.mkdirSync(path.dirname(CAP_HEALTH_BASELINE_FILE), { recursive: true });
    fs.writeFileSync(CAP_HEALTH_BASELINE_FILE, JSON.stringify({ lastBlocksToday: state.lastBlocksToday, at: new Date().toISOString() }) + '\n');
  } catch (err) { logError('Failed to persist cap-health baseline', err); }
}
loadCapHealthBaseline();

// Decide whether an upstream 429 should arm the burst cooldown, given the
// cap-health `blocksToday` before (prev) and after (current) a fresh fetch.
// Arms (true) — fail-safe — when:
//   - current is null: the fetch yielded no value (redirect/error), so we
//     can't confirm the 429 was NOT a cap block → protect the account.
//   - prev is null: no prior baseline exists (first-ever fetch) → can't prove
//     an increment either way → arm.
//   - current > prev: a real cap block was consumed since the last fetch.
// Returns false only when we have both values and current did not increase:
// the 429 was transient, not a cap hit, so the 72-minute burst cooldown would
// be overkill.
function decide429Backoff(prev, current) {
  if (current == null) return true;
  if (prev == null) return true;
  return current > prev;
}

// Fetch cap-health. Results are TTL-cached (dashboard display); a forced fetch
// (the 429 gate) bypasses the cache. Concurrent callers share a single
// in-flight request so a burst of 429s doesn't stampede app.umans.ai.
//
// `blocksToday` is null on any failure (no cookie, redirect, non-200, network
// error) so the 429 gate fails safe (arms). On success it updates
// state.lastBlocksToday (the increment baseline) and persists it.
async function fetchCapHealth({ force = false } = {}) {
  if (!state.config.sessionCookie) {
    return { ok: false, error: 'no UMANS session cookie configured', data: null, blocksToday: null };
  }
  if (!force && state.capHealthCache.data && Date.now() - state.capHealthCache.time < state.CAP_HEALTH_TTL_MS) {
    return { ...state.capHealthCache.data, cached: true };
  }
  if (state.capHealthInFlight) return state.capHealthInFlight;

  state.capHealthInFlight = (async () => {
    try {
      const resp = await fetch(CAP_HEALTH_URL, {
        headers: { Cookie: state.config.sessionCookie, Accept: 'application/json' },
        // Do NOT follow the /login redirect: a 307 means the session cookie is
        // absent/expired, which is a failure (blocksToday unknown → 429 gate
        // arms fail-safe). Following it would return the login HTML page.
        redirect: 'manual',
        signal: AbortSignal.timeout(10000),
      });
      if (resp.status !== 200) {
        const result = { ok: false, error: `cap-health returned ${resp.status}`, status: resp.status, data: null, blocksToday: null };
        state.capHealthCache = { data: result, time: Date.now() };
        return result;
      }
      const data = await resp.json();
      const blocks = typeof data?.blocksToday === 'number' ? data.blocksToday : null;
      if (blocks != null) {
        state.lastBlocksToday = blocks;
        persistCapHealthBaseline();
      }
      const result = { ok: true, data, blocksToday: blocks, fetchedAt: new Date().toISOString() };
      state.capHealthCache = { data: result, time: Date.now() };
      return result;
    } catch (err) {
      logError('cap-health fetch failed', err);
      return { ok: false, error: err.message, data: null, blocksToday: null };
    } finally {
      state.capHealthInFlight = null;
    }
  })();
  return state.capHealthInFlight;
}

module.exports = {
  fetchCapHealth,
  decide429Backoff,
  CAP_HEALTH_URL,
  CAP_HEALTH_BASELINE_FILE,
};
