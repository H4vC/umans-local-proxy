'use strict';

const state = require('./state');
const { authHeaders, upstreamURL } = require('./auth');
const { logError } = require('./http');
const { firstNumber } = require('./concurrency');

// Fetch and cache upstream /models/info (per-model reasoning capabilities).
async function fetchModelInfo({ force = false } = {}) {
  if (!force && state.modelInfoCache.data && Date.now() - state.modelInfoCache.time < state.MODEL_INFO_TTL_MS) return state.modelInfoCache.data;
  try {
    const resp = await fetch(upstreamURL('/models/info'), {
      headers: authHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) throw new Error(`/models/info returned ${resp.status}`);
    const data = await resp.json();
    state.modelInfoCache = { data, time: Date.now() };
    return data;
  } catch (err) {
    logError('UMANS /models/info fetch failed', err);
    return state.modelInfoCache.data;
  }
}

function projectStatus(raw) {
  const pick = (entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const ttftP50 = firstNumber(entry?.latency?.ttft_ms?.p50);
    const tpsP50 = firstNumber(entry?.output_tokens_per_second?.p50);
    return {
      status: String(entry?.status || 'unknown'),
      uptimePct: firstNumber(entry?.uptime_pct_24h),
      ttftMsP50: ttftP50 ?? null,
      decodeTpsP50: tpsP50 ?? null,
    };
  };
  const models = {};
  const rawModels = raw?.models || {};
  for (const id of Object.keys(rawModels)) {
    const p = pick(rawModels[id]);
    if (p) models[id] = p;
  }
  return {
    ok: true,
    overall: pick(raw?.overall),
    models,
    stale: false,
    fetchedAt: new Date().toISOString(),
  };
}

async function fetchUmansStatus({ force = false } = {}) {
  if (!state.config.apiKey) return { ok: false, error: 'UMANS API key is not configured', overall: null, models: {} };
  if (!force && state.statusCache.data && Date.now() - state.statusCache.time < state.STATUS_TTL_MS) {
    return { ...state.statusCache.data, stale: Date.now() - state.statusCache.time > state.STATUS_TTL_MS };
  }
  try {
    const resp = await fetch(upstreamURL('/status'), {
      headers: authHeaders({ Accept: 'application/json' }),
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) throw new Error(`UMANS /status returned ${resp.status}`);
    const raw = await resp.json();
    const result = projectStatus(raw);
    state.statusCache = { data: result, time: Date.now() };
    return result;
  } catch (err) {
    logError('UMANS /status fetch failed', err);
    if (state.statusCache.data) return { ...state.statusCache.data, stale: true };
    return { ok: false, error: err.message, overall: null, models: {}, stale: false };
  }
}

module.exports = {
  fetchModelInfo,
  projectStatus,
  fetchUmansStatus,
};
