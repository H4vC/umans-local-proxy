'use strict';

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const state = require('./state');
const { writeJSON, writeText, readBody, openAIError, logError } = require('./http');
const { authorized, requiresProxyAuth, filterModels, authHeaders, safeHeaders, upstreamURL } = require('./auth');
const { readJSON, cleanKeys, fileProxyApiKeys, envProxyApiKeys, parseDuration, saveConfig, CONFIG_FILE } = require('./config');
const { fetchUmansUsage, getEffectiveConcurrency, refreshUsageSoon } = require('./concurrency');
const { fetchModelInfo, fetchUmansStatus } = require('./upstream');
const { enrichModelsWithReasoning } = require('./reasoning');
const { getSessionsSnapshot, broadcastEvent } = require('./sessions');
const { proxyRequest } = require('./chat');

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

  try {
    const upstream = await fetch(upstreamURL(modelId ? `/models/${encodeURIComponent(modelId)}` : '/models'), { headers: authHeaders({ Accept: 'application/json' }), signal: AbortSignal.timeout(10000) });
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
      enabledModels: state.config.enabledModels,
      requestTimeout: state.config.requestTimeoutRaw,
      overrideConcurrency: state.config.overrideConcurrency,
      proxyAuthEnabled: state.config.proxyApiKeys.length > 0,
    });
  }

  if (req.method !== 'POST') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');

  let next;
  let apiKey = state.config.apiKey;
  let fileApiKey = state.config.fileApiKey;
  let enabledModels = state.config.enabledModels;
  let requestTimeout = state.config.requestTimeout;
  let requestTimeoutRaw = state.config.requestTimeoutRaw;
  let overrideConcurrency = state.config.overrideConcurrency;
  const fileRaw = readJSON(CONFIG_FILE);
  const fileKeys = fileProxyApiKeys(fileRaw);
  const proxyApiKeys = cleanKeys([...envProxyApiKeys(), ...fileKeys]);
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

  const nextConfig = { ...state.config, apiKey, fileApiKey, enabledModels, requestTimeout, requestTimeoutRaw, overrideConcurrency, proxyApiKeys, fileProxyApiKeys: fileKeys };
  saveConfig(nextConfig);
  state.config = nextConfig;
  state.usageCache = { data: null, time: 0 };
  state.concurrencyCache = { concurrent: null, limit: null, softLimit: null, time: 0 };
  state.statusCache = { data: null, time: 0 };
  state.modelInfoCache = { data: null, time: 0 };
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

async function handleSessions(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  writeJSON(res, 200, getSessionsSnapshot());
}

async function handleStatus(req, res) {
  if (req.method !== 'GET') return openAIError(res, 405, 'method not allowed', 'invalid_request_error');
  const force = new URL(req.url, 'http://localhost').searchParams.get('force') === '1';
  writeJSON(res, 200, await fetchUmansStatus({ force }));
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
// restarting the process. The listening socket, SSE connections, and
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
  state.sessionSeq = 0;
  state.tpsBuckets.length = 0;
  state.tpsBucketsByModel.clear();
  state.modelCharRatio.clear();
  state.messageHashCache.clear();
  state.stateMap.clear();
  state.groupSummaries.clear();
  state.seenModels.clear();
  state.usageCache = { data: null, time: 0 };
  state.concurrencyCache = { concurrent: null, limit: null, softLimit: null, time: 0 };
  state.coalesceDebug.length = 0;
  state.modelInfoCache = { data: null, time: 0 };
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
  res.write(`event: sessions\ndata: ${JSON.stringify(getSessionsSnapshot())}\n\n`);
  state.sseClients.add(res);
  const heartbeat = setInterval(() => {
    try {
      if (!res.write(': hb\n\n')) { state.sseClients.delete(res); clearInterval(heartbeat); try { res.end(); } catch {} }
    } catch { state.sseClients.delete(res); clearInterval(heartbeat); }
  }, 15000);
  req.on('close', () => { state.sseClients.delete(res); clearInterval(heartbeat); });
}

async function handleRequest(req, res, shutdown, reload) {
  const url = new URL(req.url, 'http://localhost');
  if (requiresProxyAuth(url.pathname) && !authorized(req, url)) return openAIError(res, 401, 'invalid proxy api key', 'authentication_error');
  if ((url.pathname === '/' || url.pathname === '/dashboard') && req.method === 'GET') {
    const file = path.join(__dirname, '..', 'dashboard.html');
    return writeText(res, 200, fs.readFileSync(file, 'utf8'), 'text/html; charset=utf-8');
  }
  if (url.pathname === '/health') return writeJSON(res, 200, { ok: true, upstream: state.config.upstreamBaseURL, hasApiKey: !!state.config.apiKey, proxyAuthEnabled: state.config.proxyApiKeys.length > 0, routes: ['/v1/chat/completions', '/v1/messages'] });
  if (url.pathname === '/api/events') return handleEvents(req, res);
  if (url.pathname === '/api/config') return handleConfig(req, res);
  if (url.pathname === '/api/umans/sessions') return handleSessions(req, res);
  if (url.pathname === '/api/umans/usage') return handleUsage(req, res);
  if (url.pathname === '/api/umans/concurrency') return handleConcurrency(req, res);
  if (url.pathname === '/api/umans/status') return handleStatus(req, res);
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
  handleEvents,
  handleRequest,
};
