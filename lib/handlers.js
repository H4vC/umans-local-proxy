'use strict';

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const state = require('./state');
const { writeJSON, writeText, readBody, openAIError, logError } = require('./http');
const { authorized, requiresProxyAuth, filterModels, authHeaders, safeHeaders, upstreamURL, wsUpgradeAllowed } = require('./auth');
const { readJSON, cleanKeys, fileProxyApiKeys, envProxyApiKeys, parseDuration, parseWebsearchProvider, parseRequestLogging, saveConfig, normalizeSessionCookie, CONFIG_FILE } = require('./config');
const { fetchUmansUsage, getEffectiveConcurrency, refreshUsageSoon, clearBurstCooldown } = require('./concurrency');
const { fetchModelInfo, fetchUmansStatus } = require('./upstream');
const { fetchCapHealth } = require('./cap-health');
const { enrichModelsWithReasoning } = require('./reasoning');
const { getSessionsSnapshot, broadcastEvent } = require('./sessions');
const { proxyRequest } = require('./chat');
const ws = require('./ws');

async function handleModels(req, res, modelId = null) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  if (modelId && state.config.enabledModels.length && !state.config.enabledModels.includes(modelId)) {
    return openAIError(res, 404, `model not found: ${modelId}`, 'invalid_request_error');
  }
  if (!state.config.apiKey) {
    if (!modelId) return writeJSON(res, 200, { object: 'list', data: enrichModelsWithReasoning(state.config.enabledModels.map((id) => ({ id, object: 'model', created: 0, owned_by: 'umans' }))) });
    const model = state.config.enabledModels.find((id) => id === modelId);
    if (model) return writeJSON(res, 200, enrichModelsWithReasoning([{ id: model, object: 'model', created: 0, owned_by: 'umans' }])[0]);
    return openAIError(res, 404, `model not found: ${modelId}`, 'invalid_request_error');
  }

  await fetchModelInfo();

  // Cache the /models list (not individual model fetches). The list changes
  // rarely; a 30s TTL avoids a round-trip on every dashboard refresh.
  const force = new URL(req.url, 'http://localhost').searchParams.get('force') === '1';
  if (!modelId && !force) {
    if (!state.modelsCache) state.modelsCache = { data: null, status: 0, time: 0, contentType: 'application/json' };
    if (state.modelsCache.data && Date.now() - state.modelsCache.time < state.MODELS_TTL_MS) {
      let body;
      try { body = JSON.parse(state.modelsCache.data); }
      catch { body = {}; }
      if (Array.isArray(body.data)) body.data = enrichModelsWithReasoning(filterModels(body.data));
      return writeJSON(res, state.modelsCache.status || 200, body);
    }
  }

  try {
    const upstream = await fetch(upstreamURL(modelId ? `/models/${encodeURIComponent(modelId)}` : '/models'), { headers: authHeaders({ Accept: 'application/json' }), signal: AbortSignal.timeout(10000) });
    const text = await upstream.text();
    // Cache the list response for next time.
    if (!modelId && upstream.ok) {
      if (!state.modelsCache) state.modelsCache = { data: null, status: 0, time: 0, contentType: 'application/json' };
      state.modelsCache = { data: text, status: upstream.status, time: Date.now(), contentType: upstream.headers.get('content-type') || 'application/json' };
    }
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
  if (!state.config.apiKey) return openAIError(res, 400, 'UMANS API key is not configured', 'invalid_request_error');
  try {
    const data = await fetchModelInfo({ force: new URL(req.url, 'http://localhost').searchParams.get('force') === '1' });
    if (!data) return openAIError(res, 502, 'upstream /models/info unavailable');
    writeJSON(res, 200, data);
  } catch (err) {
    openAIError(res, 502, err.message);
  }
}

async function handleConfig(req, res) {
  if (req.method === 'GET') {
    return writeJSON(res, 200, {
      listenAddr: state.config.listenAddr,
      upstreamBaseURL: state.config.upstreamBaseURL,
      hasApiKey: !!state.config.apiKey,
      hasSessionCookie: !!state.config.sessionCookie,
      enabledModels: state.config.enabledModels,
      requestTimeout: state.config.requestTimeoutRaw,
      requestLogging: state.config.requestLoggingRaw || state.config.requestLogging || 'off',
      overrideConcurrency: state.config.overrideConcurrency,
      websearchProvider: state.config.websearchProvider,
      proxyAuthEnabled: state.config.proxyApiKeys.length > 0,
    });
  }

  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');

  let next;
  let apiKey = state.config.apiKey;
  let fileApiKey = state.config.fileApiKey;
  let sessionCookie = state.config.sessionCookie;
  let fileSessionCookie = state.config.fileSessionCookie;
  let enabledModels = state.config.enabledModels;
  let requestTimeout = state.config.requestTimeout;
  let requestTimeoutRaw = state.config.requestTimeoutRaw;
  let requestLogging = state.config.requestLogging || 'off';
  let requestLoggingRaw = state.config.requestLoggingRaw || requestLogging;
  let overrideConcurrency = state.config.overrideConcurrency;
  let websearchProvider = state.config.websearchProvider;
  const fileRaw = readJSON(CONFIG_FILE);
  const fileKeys = fileProxyApiKeys(fileRaw);
  const proxyApiKeys = cleanKeys([...envProxyApiKeys(), ...fileKeys]);
  try {
    next = JSON.parse(await readBody(req) || '{}');
    if (typeof next.apiKey === 'string' && next.apiKey.trim()) {
      apiKey = next.apiKey.trim();
      fileApiKey = apiKey;
    }
    if (typeof next.sessionCookie === 'string') {
      const trimmed = next.sessionCookie.trim();
      if (trimmed) { const norm = normalizeSessionCookie(trimmed); sessionCookie = norm; fileSessionCookie = norm; }
      else { sessionCookie = ''; fileSessionCookie = ''; }
    }
    if (Array.isArray(next.enabledModels)) enabledModels = next.enabledModels.map((x) => String(x).trim()).filter(Boolean);
    if (typeof next.requestTimeout === 'string' && next.requestTimeout.trim()) {
      requestTimeoutRaw = next.requestTimeout.trim();
      requestTimeout = parseDuration(requestTimeoutRaw);
    }
    if (typeof next.requestLogging === 'string' && next.requestLogging.trim()) {
      requestLoggingRaw = next.requestLogging.trim();
      requestLogging = parseRequestLogging(requestLoggingRaw);
    }
    if (next.overrideConcurrency !== undefined) overrideConcurrency = Math.max(0, Number(next.overrideConcurrency) || 0);
    if (next.websearchProvider !== undefined) websearchProvider = parseWebsearchProvider(next.websearchProvider);
  } catch (err) {
    return openAIError(res, err.statusCode || 400, err.message, 'invalid_request_error');
  }

  const nextConfig = { ...state.config, apiKey, fileApiKey, sessionCookie, fileSessionCookie, enabledModels, requestTimeout, requestTimeoutRaw, requestLogging, requestLoggingRaw, overrideConcurrency, websearchProvider, proxyApiKeys, fileProxyApiKeys: fileKeys };
  saveConfig(nextConfig);
  state.config = nextConfig;
  state.usageCache = { data: null, time: 0 };
  state.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  state.statusCache = { data: null, time: 0 };
  state.modelInfoCache = { data: null, time: 0 };
  if (state.modelsCache) state.modelsCache = { data: null, status: 0, time: 0, contentType: 'application/json' };
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
  writeJSON(res, 200, { ...effective, active: state.activeRequests, queued: state.queuedRequests });
}

async function handleClearCooldown(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  clearBurstCooldown();
  writeJSON(res, 200, { ok: true, burstCooldown: false });
}

async function handleSessions(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, getSessionsSnapshot());
}

async function handleStatus(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  const force = new URL(req.url, 'http://localhost').searchParams.get('force') === '1';
  writeJSON(res, 200, await fetchUmansStatus({ force }));
}

async function handleCapHealth(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  const force = new URL(req.url, 'http://localhost').searchParams.get('force') === '1';
  writeJSON(res, 200, await fetchCapHealth({ force }));
}

function handleShutdown(req, res, shutdown) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, { ok: true, message: 'shutting down' });
  setImmediate(shutdown);
}

function handleRestart(req, res, shutdown) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  let child;
  try {
    child = spawn(process.execPath, [path.join(__dirname, '..', 'proxy.js')], {
      cwd: path.join(__dirname, '..'), detached: true, stdio: 'ignore', shell: false,
    });
    child.unref();
  } catch (err) {
    return openAIError(res, 500, `failed to spawn successor: ${err.message}`);
  }
  writeJSON(res, 200, { ok: true, message: 'restarting' });
  setTimeout(shutdown, 300);
}

// Hot reload: discard old handler code and re-require from disk without
// restarting the process. The listening socket, WebSocket connections, and
// in-flight requests survive. state.js is never purged, so sessions and
// caches persist across reloads.
function handleReload(req, res, reload) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  if (typeof reload !== 'function') return openAIError(res, 503, 'hot reload not available in this mode');
  const result = reload();
  if (result.ok) writeJSON(res, 200, { ok: true, message: 'code reloaded', reloadCount: state.reloadCount });
  else writeJSON(res, 500, { ok: false, error: result.error });
}

function handleClearState(req, res) {
  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  const cleared = {
    sessions: state.sessions.size,
    groups: state.groupSummaries.size,
    seenModels: state.seenModels.size,
    stateMap: state.stateMap.size,
    messageHashCache: state.messageHashCache.size,
    coalesceDebug: state.coalesceDebug.length,
  };
  state.sessions.clear();
  if (state.sessionsByGroup) state.sessionsByGroup.clear();
  state.sessionSeq = 0;
  state.modelCharRatio.clear();
  state.messageHashCache.clear();
  for (const t of state.stateMapTimers.values()) clearTimeout(t);
  state.stateMapTimers.clear();
  state.stateMap.clear();
  for (const t of state.groupSummaryTimers.values()) clearTimeout(t);
  state.groupSummaryTimers.clear();
  state.groupSummaries.clear();
  state.seenModels.clear();
  state.usageCache = { data: null, time: 0 };
  state.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  state.usageEverFetched = false; // C2: cleared cache re-engages cold-start protection until next /usage succeeds
  state.coalesceDebug.length = 0;
  state.modelInfoCache = { data: null, time: 0 };
  if (state.modelsCache) state.modelsCache = { data: null, status: 0, time: 0, contentType: 'application/json' };
  clearInterval(state.sessionsBroadcastTimer);
  state.sessionsBroadcastTimer = null;
  state.broadcastThrottled = false;
  broadcastEvent('sessions', getSessionsSnapshot());
  writeJSON(res, 200, { ok: true, cleared });
}

function handleSystemInfo(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, {
    pid: process.pid,
    startedAt: new Date(state.startedAt).toISOString(),
    uptimeMs: Date.now() - state.startedAt,
    listenAddr: state.config.listenAddr,
    upstream: state.config.upstreamBaseURL,
    nodeVersion: process.version,
    activeRequests: state.activeRequests,
    queuedRequests: state.queuedRequests,
    sessionsTracked: state.sessions.size,
    reloadCount: state.reloadCount,
    lastReloadAt: state.lastReloadAt,
    tapWorkerDead: state.tapWorkerDead === true,
  });
}
function handleUpgrade(req, socket) {
  if (!wsUpgradeAllowed(req)) {
    try { socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); } catch {}
    socket.destroy();
    return;
  }
  if (!ws.handshake(req, socket)) return;
  ws.startWsKeepalive(); // C1: idempotent — arms the keepalive ping sweep on first WS client
  const client = ws.addClient(socket);
  state.wsClients.add(client);

  // Send initial state immediately.
  ws.send(client, JSON.stringify({ type: 'connected', data: { time: Date.now() } }));
  ws.send(client, JSON.stringify({ type: 'sessions', data: getSessionsSnapshot() }));

  // Parse incoming frames. We don't expect client messages, but we handle
  // ping/pong and close frames per the RFC.
  // Amortized-growth receive buffer (avoids O(n²) per-chunk Buffer.concat
  // under a dribbling client) capped at MAX_FRAME_SIZE. parseFrame rejects
  // unmasked/oversized-control frames as 'protocol' and oversized frames as
  // 'tooLarge'; both close the connection.
  let store = Buffer.allocUnsafe(4096);
  let used = 0;
  socket.on('data', (chunk) => {
    const need = used + chunk.length;
    if (need > store.length) {
      let cap = store.length;
      while (cap < need) cap *= 2;
      const grown = Buffer.allocUnsafe(cap);
      store.copy(grown, 0, 0, used);
      store = grown;
    }
    chunk.copy(store, used);
    used += chunk.length;
    if (used > ws.MAX_FRAME_SIZE) { ws.close(client); state.wsClients.delete(client); socket.destroy(); return; }
    let off = 0;
    while (off < used) {
      const frame = ws.parseFrame(store.subarray(off, used));
      if (!frame) break;
      if (frame.type === 'tooLarge' || frame.type === 'protocol') { ws.close(client); state.wsClients.delete(client); socket.destroy(); return; }
      if (frame.type === 0x08) { ws.close(client); state.wsClients.delete(client); return; } // close
      if (frame.type === 0x09) ws.pong(client, frame.data); // ping → pong (RFC 6455 §5.5.2)
      if (frame.type === 0x0A) client.awaitingPong = false; // pong → clear keepalive flag (C1)
      // text (0x01), continuation (0x00) — ignored (no client→server msgs expected)
      off += frame.consumed;
    }
    if (off > 0) { store.copy(store, 0, off, used); used -= off; }
  });

  socket.on('close', () => { state.wsClients.delete(client); });
  socket.on('error', () => { state.wsClients.delete(client); });
}

async function handleRequest(req, res, shutdown, reload) {
  const url = new URL(req.url, 'http://localhost');
  if (requiresProxyAuth(url.pathname) && !authorized(req)) return openAIError(res, 401, 'invalid proxy api key', 'authentication_error');
  if ((url.pathname === '/' || url.pathname === '/dashboard') && req.method === 'GET') {
    const file = path.join(__dirname, '..', 'dashboard.html');
    // Lazy-init: state.dashboardCache may not exist if state singleton was
    // created before this field was added (hot reload scenario).
    if (!state.dashboardCache) state.dashboardCache = { html: null, mtime: 0 };
    let html = state.dashboardCache.html;
    try {
      const stat = fs.statSync(file);
      if (stat.mtimeMs !== state.dashboardCache.mtime) {
        html = fs.readFileSync(file, 'utf8');
        state.dashboardCache = { html, mtime: stat.mtimeMs };
      }
    } catch {
      if (!html) html = fs.readFileSync(file, 'utf8');
    }
    return writeText(res, 200, html, 'text/html; charset=utf-8');
  }
  if (url.pathname === '/health') return writeJSON(res, 200, { ok: true, upstream: state.config.upstreamBaseURL, hasApiKey: !!state.config.apiKey, proxyAuthEnabled: state.config.proxyApiKeys.length > 0, routes: ['/v1/chat/completions', '/v1/messages'] });
  if (url.pathname === '/api/config') return handleConfig(req, res);
  if (url.pathname === '/api/umans/sessions') return handleSessions(req, res);
  if (url.pathname === '/api/umans/usage') return handleUsage(req, res);
  if (url.pathname === '/api/umans/concurrency') return handleConcurrency(req, res);
  if (url.pathname === '/api/umans/cooldown/clear') return handleClearCooldown(req, res);
  if (url.pathname === '/api/umans/status') return handleStatus(req, res);
  if (url.pathname === '/api/umans/cap-health') return handleCapHealth(req, res);
  if (url.pathname === '/v1/models/info') return handleModelsInfo(req, res);
  if (url.pathname === '/v1/models') return handleModels(req, res);
  if (url.pathname.startsWith('/v1/models/')) return handleModels(req, res, decodeURIComponent(url.pathname.slice('/v1/models/'.length)));
  if (url.pathname === '/v1/chat/completions') return proxyRequest(req, res, { shape: 'openai' });
  if (url.pathname === '/v1/messages') return proxyRequest(req, res, { shape: 'anthropic' });
  if (url.pathname.startsWith('/v1/')) return openAIError(res, 404, `unsupported endpoint: ${url.pathname}`, 'invalid_request_error');
  if (url.pathname === '/api/shutdown') return handleShutdown(req, res, shutdown);
  if (url.pathname === '/api/restart') return handleRestart(req, res, shutdown);
  if (url.pathname === '/api/reload') return handleReload(req, res, reload);
  if (url.pathname === '/api/clear-state') return handleClearState(req, res);
  if (url.pathname === '/api/debug/coalesce') return writeJSON(res, 200, state.coalesceDebug);
  if (url.pathname === '/api/system/info') return handleSystemInfo(req, res);
  writeText(res, 404, 'Not Found');
}

module.exports = {
  handleModels,
  handleModelsInfo,
  handleConfig,
  handleUsage,
  handleConcurrency,
  handleSessions,
  handleStatus,
  handleShutdown,
  handleRestart,
  handleReload,
  handleClearState,
  handleSystemInfo,
  handleUpgrade,
  handleRequest,
};
