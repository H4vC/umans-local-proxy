'use strict';

// Singleton holding all mutable proxy state. Every module imports this
// instead of declaring its own module-level let/const — so state survives
// across require.cache invalidation (hot reload) and remains consistent
// between modules. Values are mutated in place (e.g. state.config = next),
// never replaced, so all holders see updates.

module.exports = {
  // ---- config ----
  config: null, // set by server.js at boot via loadConfig()

  // ---- timing constants ----
  startedAt: 0,
  reloadCount: 0,
  lastReloadAt: null,
  MAX_BODY_SIZE: 25 * 1024 * 1024,
  USAGE_TTL_MS: 10 * 1000,
  MODEL_INFO_TTL_MS: 60 * 1000,
  MODELS_TTL_MS: 30 * 1000,
  TPS_WINDOW_MS: 5000,
  COALESCE_DEBUG_MAX: 50,
  COALESCE_TTL_MS: 60 * 60 * 1000,
  GROUP_SUMMARY_TTL_MS: 60 * 60 * 1000,
  SESSION_BROADCAST_MIN_MS: 1000,
  REFRESH_USAGE_MIN_MS: 10000,

  // ---- upstream caches ----
  usageCache: { data: null, time: 0 },
  concurrencyCache: { concurrent: null, limit: null, softLimit: null, time: 0 },
  statusCache: { data: null, time: 0 },
  modelInfoCache: { data: null, time: 0 },
  modelsCache: { data: null, status: 0, time: 0 },

  // ---- throttle / request tracking ----
  activeRequests: 0,
  queuedRequests: 0,
  inFlightControllers: new Set(),

  // ---- SSE ----
  sseClients: new Set(),

  // ---- sessions / telemetry ----
  sessions: new Map(),
  sessionSeq: 0,
  tpsBuckets: [],
  tpsBucketsByModel: new Map(),
  seenModels: new Set(),
  modelCharRatio: new Map(),
  groupSummaries: new Map(),
  sessionsBroadcastTimer: null,
  broadcastThrottled: false,

  // ---- coalescing ----
  messageHashCache: new Map(),
  stateMap: new Map(),
  coalesceDebug: [],
  refreshUsageInFlight: false,
  refreshUsageTimer: null,

  // ---- server lifecycle (set by lib/server.js, survives reloads) ----
  httpServer: null,    // the http.Server instance — created once, never replaced
  listening: false,    // guard: listen + error handler registered once
  handleRequest: null, // mutable handler reference — swapped on reload
  shutdown: null,      // graceful shutdown function
  reload: null,        // hot reload function
};
