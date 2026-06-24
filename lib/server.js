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
const { loadConfig, parseListenAddr } = require('./config');
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
  for (const p of reloadablePaths()) delete require.cache[p];
  try {
    // Re-require server.js first so it updates state.shutdown/reload with
    // the new code, then call boot() which re-requires handlers and swaps
    // the handler reference (detecting the existing httpServer, skipping listen).
    const newServer = require('./server');
    newServer.boot();
    state.reloadCount++;
    state.lastReloadAt = new Date().toISOString();
    console.log(`[reload] code swapped (reload #${state.reloadCount})`);
    return { ok: true };
  } catch (err) {
    // boot() sets state.shutdown/reload AFTER require('./handlers'), so if
    // that throws they still hold the old values — no rollback needed for
    // those. state.handleRequest is restored as defense-in-depth.
    state.handleRequest = oldHandler;
    console.error(`[reload] FAILED — keeping old code: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

function shutdown() {
  console.log('Shutting down UMANS Proxy');
  if (state.httpServer) state.httpServer.close(() => process.exit(0));
  for (const res of state.sseClients) { try { res.end(); } catch {} state.sseClients.delete(res); }
  for (const c of state.inFlightControllers) { try { c.abort(); } catch {} }
  setTimeout(() => process.exit(0), 5000).unref();
}

function boot() {
  // Initialize config + startedAt before any module touches them.
  // Only do this on first boot — on reload, config is already live.
  if (!state.config) {
    state.config = loadConfig();
    state.startedAt = Date.now();
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
  state.shutdown = shutdown;
  state.reload = reload;

  // First boot: create the server and start listening.
  // On reload: reuse the existing server — just the handler reference changed.
  if (state.httpServer && state.listening) {
    console.log(`[reload] server lifecycle updated`);
    return;
  }

  const server = http.createServer((req, res) => {
    state.handleRequest(req, res, state.shutdown, state.reload).catch((err) => {
      logError('Request handler failed', err);
      if (!res.headersSent) openAIError(res, 500, err.message);
      else res.end();
    });
  });

  state.httpServer = server;

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      server.listenRetries = (server.listenRetries || 0) + 1;
      if (server.listenRetries > 40) { console.error('Giving up: port still in use after 20s'); process.exit(1); }
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
