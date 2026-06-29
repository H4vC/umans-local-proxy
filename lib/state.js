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
  STATUS_TTL_MS: 30 * 1000,
  MODELS_TTL_MS: 30 * 1000,
  TPS_WINDOW_MS: 5000,
  COALESCE_DEBUG_MAX: 50,
  COALESCE_TTL_MS: 60 * 60 * 1000,
  GROUP_SUMMARY_TTL_MS: 60 * 60 * 1000,
  SESSION_TTL_MS: 5 * 60 * 1000,
  SESSION_BROADCAST_MIN_MS: 1000,
  REFRESH_USAGE_MIN_MS: 10000,

  // ---- upstream caches ----
  usageCache: { data: null, time: 0 },
  concurrencyCache: { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 },
  // True once /usage has been successfully fetched at least once. Before this,
  // a null concurrency limit is ignorance (not "unlimited") — acquireThrottleSlot
  // admits only a minimal floor to avoid an unthrottled relay during a /usage
  // outage (C2).
  usageEverFetched: false,
  // Timestamp (ms) until which bursting is disabled after an upstream 429.
  // The cooldown duration is 24h / 20 = 1.2h (72 minutes): a 429 from
  // upstream arms this, and while it is in the future the effective
  // concurrency limit is clamped to the soft limit (no bursting). A live
  // UMANS priority box also clamps to the soft limit via boxedUntil.
  burstDisabledUntil: 0,
  statusCache: { data: null, time: 0 },
  modelInfoCache: { data: null, time: 0 },
  modelsCache: { data: null, status: 0, time: 0 },
  // ---- cap-health (app.umans.ai account endpoint) ----
  // TTL cache for dashboard display; bypassed by a forced fetch on 429.
  capHealthCache: { data: null, time: 0 },
  CAP_HEALTH_TTL_MS: 30 * 1000,
  // Shared in-flight promise: debounces concurrent fetchers (429 gate +
  // dashboard poll) so a burst of 429s makes one request, not many.
  capHealthInFlight: null,
  // Last confirmed blocksToday from cap-health — the increment baseline for
  // the 429 backoff gate. Persisted to .runtime/cap-health-baseline.json so a
  // process restart doesn't lose the high-water mark and re-arm spuriously.
  lastBlocksToday: null,

  // ---- throttle / request tracking ----
  activeRequests: 0,
  queuedRequests: 0,
  throttleWaiters: [],
  inFlightControllers: new Set(),

  // ---- WebSocket live updates ----
  wsClients: new Set(),
  wsKeepaliveTimer: null, // periodic ping sweep + pong-timeout close (C1); survives reload

  // ---- sessions / telemetry ----
  sessions: new Map(),
  sessionsByGroup: new Map(), // groupKey → Set<session id>; cache-miss eviction index (O(group), not O(all))
  sessionSeq: 0,
  seenModels: new Set(),
  modelCharRatio: new Map(),
  groupSummaries: new Map(),
  groupSummaryTimers: new Map(), // groupKey → Timeout, cancelled on refresh to prevent accumulation
  sessionsBroadcastTimer: null,
  broadcastThrottled: false,

  // ---- tap worker (survives reload so it isn't orphaned) ----
  tapWorker: null,           // Worker handle, created lazily by lib/chat-tap.js
  tapWorkerDead: false,      // set when the worker errored/exited; restarted lazily
  tapPendingFinals: new Map(), // id -> { resolve, timer } for in-flight onEnd() round-trips
  tapRestartTimer: null,      // backoff timer before recreating a dead worker
  tapActiveTaps: new Map(), // id -> { shape, model, stream } for in-flight taps; re-init'd on worker restart (C8)

  // ---- coalescing ----
  messageHashCache: new Map(),
  stateMap: new Map(),
  stateMapTimers: new Map(), // stateChain → Timeout, cleared on overwrite to prevent stale eviction
  coalesceDebug: [],
  refreshUsageInFlight: false,
  refreshUsageTimer: null,
  usageBackgroundTimer: null,   // background refresh timer for usage cache

  // ---- server lifecycle (set by lib/server.js, survives reloads) ----
  httpServer: null,    // the http.Server instance — created once, never replaced
  listening: false,    // guard: listen + error handler registered once
  handleRequest: null, // mutable handler reference — swapped on reload
  handleUpgrade: null, // mutable WS upgrade handler — swapped on reload
  reload: null,        // hot reload function
};
