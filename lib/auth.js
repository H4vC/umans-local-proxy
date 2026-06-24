'use strict';

const crypto = require('crypto');
const state = require('./state');

function upstreamURL(suffix) {
  return `${state.config.upstreamBaseURL}${suffix}`;
}

function authHeaders(extra = {}, { anthropic = false } = {}) {
  const headers = { ...extra };
  if (anthropic) {
    if (state.config.apiKey) headers['x-api-key'] = state.config.apiKey;
    headers['anthropic-version'] = '2023-06-01';
  } else if (state.config.apiKey) {
    headers.Authorization = `Bearer ${state.config.apiKey}`;
  }
  return headers;
}

function safeHeaders(headers) {
  const out = {};
  for (const [key, value] of headers) {
    const lower = key.toLowerCase();
    if (['connection', 'content-encoding', 'keep-alive', 'transfer-encoding', 'content-length'].includes(lower)) continue;
    out[key] = value;
  }
  return out;
}

function timingSafeEqual(a, b) {
  // Hash both sides to fixed-length digests before comparing, so neither
  // an early return nor comparison timing leaks the key length.
  const ah = crypto.createHash('sha256').update(String(a), 'utf8').digest();
  const bh = crypto.createHash('sha256').update(String(b), 'utf8').digest();
  return crypto.timingSafeEqual(ah, bh);
}

function authorized(req, url) {
  if (!state.config.proxyApiKeys.length) return true;
  const tokens = [req.headers['x-api-key'], req.headers.authorization, url?.searchParams.get('key')]
    .flatMap((value) => Array.isArray(value) ? value : [value])
    .map((value) => String(value || '').replace(/^Bearer\s+/i, '').trim())
    .filter(Boolean);
  return tokens.some((token) => state.config.proxyApiKeys.some((key) => timingSafeEqual(token, key)));
}

function requiresProxyAuth(pathname) {
  return pathname === '/api/config' || pathname === '/api/events' || pathname === '/api/shutdown' || pathname === '/api/restart' || pathname === '/api/reload' || pathname === '/api/clear-state' || pathname === '/api/system/info' || pathname === '/api/debug/coalesce' || pathname.startsWith('/api/umans/') || pathname.startsWith('/v1/');
}

function filterModels(models) {
  if (!state.config.enabledModels.length) return models;
  const allowed = new Set(state.config.enabledModels);
  return models.filter((model) => allowed.has(model.id || model));
}

module.exports = {
  upstreamURL,
  authHeaders,
  safeHeaders,
  authorized,
  requiresProxyAuth,
  filterModels,
};
