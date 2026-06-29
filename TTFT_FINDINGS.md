# TTFT findings

Evaluation date: 2026-06-29. Scope: local Node.js UMANS proxy request path for `POST /v1/chat/completions` and `POST /v1/messages`.

## Baseline observed

`node bench/hotpath.js`:

- Big buffer pipe + tap: `679.4 MB/s` / `5.31 Gbps`
- Big buffer pipe only: `1927.8 MB/s` / `15.06 Gbps`
- Small chunks pipe + tap: `237.6 MB/s` / `1.86 Gbps`
- Small chunks pipe only: `875.7 MB/s` / `6.84 Gbps`

## Ordered opportunities

### 1. Avoid cold `/models/info` on `reasoning_effort`

Files: `lib/chat.js`, `lib/upstream.js`, `lib/server.js`.

Any OpenAI request with `reasoning_effort` awaits `fetchModelInfo()` before upstream fetch. Boot pre-warm helps, but cache expiry can still put `/models/info` on the TTFT path.

Recommendation: make chat use stale-while-revalidate model info. Return existing cache immediately even past TTL, refresh in background, and only block when no cache exists.

Risk: reasoning capability data may be stale briefly.

### 2. Move or optimize pre-upstream coalescing hash

Files: `lib/chat.js`, `lib/coalesce.js`.

`resolveGroupKey()` walks the prompt prefix and hashes canonical messages before the upstream request starts. Large conversations or uncached large prompts pay CPU before TTFT.

Recommendation: defer grouping until after upstream fetch starts, or cache large message hashes by bounded digest instead of retaining full serialized prompt keys.

Risk: session grouping/coalescing may be delayed or less exact if deferred.

### 3. Reduce full-body parse cost

Files: `lib/chat.js`, `lib/http.js`.

The proxy buffers the full request body and `JSON.parse`s it before upstream fetch. This is necessary for current validation/mutation, but expensive for large prompts.

Recommendation: fast-scan only top-level fields needed for routing/mutation (`model`, `stream`, `reasoning_effort`, `stream_options`) and full-parse only when enabled-model filtering, reasoning snapping, or coalescing actually needs it.

Risk: JSON scanner correctness.

### 4. Defer telemetry setup until upstream response

Files: `lib/chat.js`, `lib/chat-tap.js`, `lib/sessions.js`.

The proxy creates a session, initializes `ChatTap`, posts to the worker, and broadcasts session start before upstream fetch.

Recommendation: create `ChatTap` immediately before piping `upstream.body`; move dashboard broadcasts to post-upstream or `setImmediate`.

Risk: dashboard active-session display appears slightly later.

### 5. Reduce dashboard/session event-loop contention

Files: `lib/sessions.js`, `lib/ws.js`, `dashboard.html`.

Session broadcasts compute full snapshots, sort sessions/models/groups, stringify payloads, and write to each WebSocket client on the same event loop as chat.

Recommendation: skip snapshot work when no WebSocket clients exist; send deltas for active-session updates; keep full snapshots on slower intervals or explicit dashboard connect.

Risk: more UI state handling complexity.

### 6. Replace Transform tap with tee/manual pump if profiling warrants

Files: `lib/chat-tap.js`, `lib/chat.js`.

The worker offload moved parsing off-thread, but the main-thread Transform and chunk-copy path still leaves pipe+tap far below pure-pipe throughput.

Recommendation: tee the upstream stream: write original chunks directly to `res`, and send telemetry copies to the worker on a side path while preserving backpressure and abort semantics.

Risk: manual pump must be carefully tested for abort/backpressure correctness.

### 7. Avoid full-string copy for `stream_options.include_usage`

File: `lib/chat.js`.

The splice is cheaper than `JSON.stringify(payload)`, but still creates a full new request string.

Recommendation: make exact usage optional for TTFT mode, or use a streamed request body composed from prefix + inserted option + suffix.

Risk: missing exact usage if disabled; streamed request body needs Node-version coverage.

### 8. Avoid blocking on `/usage` when limit is unknown

File: `lib/concurrency.js`.

When effective concurrency limit is unknown and there are active requests, `acquireThrottleSlot()` can await `/usage`.

Recommendation: persist last known concurrency across restarts and refresh `/usage` in background rather than blocking chat.

Risk: temporary oversubscription after stale limits.

### 9. Keep upstream status preservation despite TTFT tradeoff

File: `lib/chat.js`.

Streaming headers now wait for upstream status so OMP sees real `429`/`403`. Do not reintroduce early `200` SSE error masking.

Recommendation: after upstream returns `2xx`, write headers immediately before telemetry/dashboard work.
