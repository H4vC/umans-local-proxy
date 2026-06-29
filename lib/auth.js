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
  // The app.umans.ai session cookie is intentionally NOT attached here — it
  // authenticates a different host/scheme (app.umans.ai), not the api.code.umans.ai
  // chat/usage/models upstream. cap-health.js sets it on its own app.umans.ai fetch.
  return headers;
}

// Encodings undici auto-decodes in the response stream. For these the body
// reaching the client is already plaintext, so the content-encoding header
// (and the now-wrong content-length) MUST be stripped or the client
// double-decodes. The upstream chat request advertises only these via
// Accept-Encoding, so they are all that should ever arrive (C7).
const DECODED_ENCODINGS = new Set(['gzip', 'deflate', 'br']);

function safeHeaders(headers) {
  const out = {};
  let encoding = null;
  for (const [key, value] of headers) {
    const lower = key.toLowerCase();
    if (lower === 'content-encoding') { encoding = String(value).toLowerCase().trim(); continue; }
    if (['connection', 'keep-alive', 'transfer-encoding', 'content-length'].includes(lower)) continue;
    out[key] = value;
  }
  // Keep content-encoding (and content-length, deferred above) only for
  // encodings undici does NOT decode (zstd on Node <22, or unknown): the body
  // is still compressed, so the header tells the client to decode. For decoded
  // encodings both stay stripped (plain body, length unknown) — the prior
  // unconditional strip silently corrupted non-decoded bodies (C7).
  if (encoding && !DECODED_ENCODINGS.has(encoding)) {
    out['content-encoding'] = encoding;
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

function authorized(req) {
  if (!state.config.proxyApiKeys.length) return true;
  // Header-only: x-api-key or Authorization (Bearer). The ?key= query path is
  // intentionally NOT accepted — it would leak the proxy key into access logs
  // and browser history. The dashboard sends x-api-key.
  const tokens = [req.headers['x-api-key'], req.headers.authorization]
    .flatMap((value) => Array.isArray(value) ? value : [value])
    .map((value) => String(value || '').replace(/^Bearer\s+/i, '').trim())
    .filter(Boolean);
  return tokens.some((token) => state.config.proxyApiKeys.some((key) => timingSafeEqual(token, key)));
}

function isLoopback(req) {
  const addr = req.socket?.remoteAddress;
  return addr === '127.0.0.1' || addr === '::1' || addr === '::ffff:127.0.0.1';
}

// Cross-site WebSocket hijacking (CSWSH) defense for the /ws upgrade. Browsers
// can't set custom headers on a WebSocket, so the dashboard (served same-origin
// by this proxy) authenticates via its Origin: a same-origin browser's Origin
// authority equals the Host header, while a CSWSH attempt from another site
// does not. When auth is enabled, non-browser clients may instead present a
// proxy key via header. No-Origin loopback (local scripts) is trusted; all
// else is rejected.
function wsUpgradeAllowed(req) {
  const origin = req.headers.origin;
  if (origin) {
    try { if (new URL(origin).host === req.headers.host) return true; } catch {}
    return false; // cross-origin browser → reject (CSWSH)
  }
  if (isLoopback(req)) return true; // local non-browser
  if (state.config.proxyApiKeys.length) return authorized(req); // non-browser + key (header)
  return false;
}

function requiresProxyAuth(pathname) {
  return pathname === '/api/config' || pathname === '/api/shutdown' || pathname === '/api/restart' || pathname === '/api/reload' || pathname === '/api/clear-state' || pathname === '/api/system/info' || pathname === '/api/debug/coalesce' || pathname.startsWith('/api/umans/') || pathname.startsWith('/v1/');
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
  isLoopback,
  wsUpgradeAllowed,
};
