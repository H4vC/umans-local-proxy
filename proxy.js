'use strict';

// Immutable bootstrap. Creates the process, wires signals, and calls
// lib/server.js boot(). All server logic lives in lib/ and is hot-reloadable
// via POST /api/reload. This file should never need to change.

const { boot, shutdown } = require('./lib/server');

boot();

process.once('SIGINT', () => shutdown());
process.once('SIGTERM', () => shutdown());

// C10: process-level safety net. A stray throw from a timer callback (notably
// broadcastEvent → ws.send, now also try/catch'd in ws.js) would otherwise
// escape and, per Node 15+ default, crash the proxy mid-traffic. Log + continue
// so one bad broadcast/socket doesn't take the process down (matches the tap
// worker, which already does this). proxy.js runs once, so these don't
// accumulate across hot reloads.
process.on('uncaughtException', (err) => { console.error('[uncaughtException]', err?.stack || err); });
process.on('unhandledRejection', (err) => { console.error('[unhandledRejection]', err?.stack || err); });
