'use strict';

// Server lifecycle module. Re-requirable: on first call it creates the
// http.Server and starts listening. On subsequent calls (hot reload) it
// detects the existing server in state.httpServer, skips creation, and
// only swaps the handler reference + lifecycle functions.
//
// proxy.js is a ~10-line bootstrap that calls boot() once and wires
// SIGINT/SIGTERM. Everything else lives here and is reloadable.

const http = require('http');
const path = require('path');

const state = require('./state');
const { loadConfig, parseListenAddr, isLoopbackHost } = require('./config');
const { logError, openAIError } = require('./http');

const LIB_DIR = __dirname; // server.js lives in lib/

// Paths that must be purged from require.cache on reload.
function reloadablePaths() {
  const entries = Object.keys(require.cache);
  return entries.filter((p) => {
    if (!p.startsWith(LIB_DIR)) return false;
    // Never purge state.js — it holds all live mutable state + httpServer.
    if (p === path.join(LIB_DIR, 'state.js')) return false;
    return true;
  });
}

function reload() {
  const oldHandler = state.handleRequest;
  const oldConfig = state.config;
  for (const p of reloadablePaths()) delete require.cache[p];
  // Re-read config.json so external edits (API keys, listen addr, enabled
  // models) take effect without a restart. A parse error keeps the prior config
  // — reload is non-fatal, unlike boot's refuse-to-boot (B2).
  try {
    state.config = require('./config').loadConfig();
  } catch (err) {
    console.error(`[reload] config re-read failed, keeping prior config: ${err.message}`);
    state.config = oldConfig;
  }
  try {
    // Re-require server.js first so it updates state.shutdown/reload with
    // the new code, then call boot() which re-requires handlers and swaps
    // the handler reference (detecting the existing httpServer, skipping listen).
    const newServer = require('./server');
    newServer.boot();
    // Re-arm the sessions broadcast timer. The existing interval (set by the
    // previous sessions.js module) closes over that module's getSessionsSnapshot,
    // so WS pushes would keep using stale logic until a new request re-arms it.
    // Clear the old broadcast interval so the NEW sessions.js re-arms it with
    // its own getSessionsSnapshot closure. Without this the guard in
    // scheduleSessionsBroadcast skips while sessions are active, and the stale
    // (cache-purged) closure keeps emitting snapshots until idle.
    try {
      if (state.sessionsBroadcastTimer) { clearInterval(state.sessionsBroadcastTimer); state.sessionsBroadcastTimer = null; }
      require('./sessions').scheduleSessionsBroadcast();
    } catch {}
    state.reloadCount++;
    state.lastReloadAt = new Date().toISOString();
    console.log(`[reload] code swapped (reload #${state.reloadCount})`);
    return { ok: true };
  } catch (err) {
    // boot() sets state.shutdown/reload AFTER require('./handlers'), so if
    // that throws they still hold the old values — no rollback needed for
    // those. state.handleRequest + state.config are restored as defense-in-depth.
    state.handleRequest = oldHandler;
    state.config = oldConfig;
    console.error(`[reload] FAILED — keeping old code: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

function shutdown() {
  console.log('Shutting down UMANS Proxy');
  if (state.httpServer) {
    // C9: drop all connections (incl. idle HTTP keep-alive) so the listening
    // socket frees promptly for a successor process. Without this, close()
    // waits on lingering keep-alive sockets and the successor's EADDRINUSE
    // retries can exhaust before the port is released.
    if (typeof state.httpServer.closeAllConnections === 'function') state.httpServer.closeAllConnections();
    state.httpServer.close(() => process.exit(0));
  }
  for (const c of state.inFlightControllers) { try { c.abort(); } catch {} }
  for (const c of state.wsClients) { try { require('./ws').close(c); } catch {} }
  setTimeout(() => process.exit(0), 5000).unref();
}

function boot() {
  // Initialize config + startedAt before any module touches them.
  // Only do this on first boot — on reload, config is already live.
  if (!state.config) {
    try {
      state.config = loadConfig();
    } catch (err) {
      console.error(`Config error: ${err.message}`);
      process.exit(1);
    }
    state.startedAt = Date.now();
    // Pre-warm the model-info cache so the first reasoning_effort request
    // doesn't pay the /models/info fetch on the critical path — fetchModelInfo
    // returns synchronously from cache when warm. Fire-and-forget: boot
    // proceeds and the cache warms in the background; a failure is logged and
    // cached as null, degrading gracefully.
    try { require('./upstream').fetchModelInfo().catch(() => {}); } catch {}
    // Pre-warm the usage cache so acquireThrottleSlot never blocks on a cold
    // cache — the background timer keeps it fresh between requests.
    try { require('./concurrency').startUsageBackgroundRefresh(); } catch {}
   }

  let host, port;
  try {
    ({ host, port } = parseListenAddr(state.config.listenAddr));
  } catch (err) {
    console.error(`Startup error: ${err.message}`);
    process.exit(1);
  }

  // Always update the mutable lifecycle functions + handler reference.
  // On reload this is the actual code swap.
  state.handleRequest = require('./handlers').handleRequest;
  state.handleUpgrade = require('./handlers').handleUpgrade;
  state.shutdown = shutdown;
  state.reload = reload;

  // First boot: create the server and start listening.
  // On reload: reuse the existing server — just the handler reference changed.
  if (state.httpServer && state.listening) {
    // Reload path: the old tap worker's event handlers are stale closures from
    // the previous chat-tap.js module. Restart the worker so the new code's
    // onWorkerMessage (with op:error + exit-code-0 fixes) is registered.
    try { require('./chat-tap').restartTapWorker(); } catch {}
    console.log(`[reload] server lifecycle updated`);
    return;
  }

  // Refuse to expose a non-loopback bind without proxy auth — otherwise every
  // admin endpoint and the upstream relay would be open to the network.
  if (!isLoopbackHost(host) && state.config.proxyApiKeys.length === 0) {
    console.error(`Refusing to bind non-loopback ${host}:${port} with no API_KEYS configured — set API_KEYS or bind 127.0.0.1.`);
    process.exit(1);
  }

  const server = http.createServer((req, res) => {
    state.handleRequest(req, res, state.shutdown, state.reload).catch((err) => {
      logError('Request handler failed', err);
      if (!res.headersSent) openAIError(res, 500, err.message);
      else res.end();
    });
  });

  state.httpServer = server;

  // WebSocket upgrade — wired once on first boot, survives reloads.
  server.on('upgrade', (req, socket) => {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') { socket.destroy(); return; }
    try { state.handleUpgrade(req, socket); } catch (err) { logError('WS upgrade failed', err); socket.destroy(); }
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      server.listenRetries = (server.listenRetries || 0) + 1;
      if (server.listenRetries > 60) { console.error('Giving up: port still in use after 30s'); process.exit(1); }
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

  try {
    server.listen({ port, host, exclusive: true }, () => {
      console.log(`UMANS Proxy listening on http://${host}:${port}`);
      console.log(`Upstream: ${state.config.upstreamBaseURL}`);
      console.log(`API key: ${state.config.apiKey ? 'configured' : 'missing'}`);
      console.log(`Hot reload: POST /api/reload`);
    });
    state.listening = true;
  } catch (err) {
    console.error(`Startup error: ${err.message}`);
    process.exit(1);
  }
}

module.exports = { boot, reload, shutdown };
