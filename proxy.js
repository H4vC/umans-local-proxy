'use strict';

// Immutable bootstrap. Creates the process, wires signals, and calls
// lib/server.js boot(). All server logic lives in lib/ and is hot-reloadable
// via POST /api/reload. This file should never need to change.

const { boot, shutdown } = require('./lib/server');

boot();

process.once('SIGINT', () => shutdown());
process.once('SIGTERM', () => shutdown());
