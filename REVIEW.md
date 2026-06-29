# UMANS Proxy — full repo review

**Scope:** 22 lib files + 4 test files + dashboard, ~4300 LOC, zero deps (pure Node stdlib).
**Method:** four parallel subsystem reviews (concurrency, hot path, infra/security, sessions/tests); each finding cross-checked against first-hand reads of the cited lines. Findings personally verified are marked ✓.

---

## A. Must-fix bugs (active correctness defects)

### A1. Live TPS is wrong for any session older than 5s ✓
`lib/sessions.js:122-123`
`sessionTps` divides **cumulative** `outputTokens` (a running total, never pruned — `tap-worker.js:136`) by `Math.min(elapsed, 5000)`. For a steady 10 tok/s stream, the displayed rate *grows without bound*: 60 tok/s at 30s, 120 at 60s. It is neither an average nor a rolling window despite the README claiming "rolling 5s window." The dashboard's headline metric is wrong. `sessions.test.js:24-26` bakes in the bug (`rate(250)=50` for a 10s session whose true average is 25).
**Fix:** track a sliding 5s window of `(ts, Δtokens)` and sum within it, or switch to `outputTokens/elapsed*1000` and drop the cap. Fix the test to assert the real value.

### A2. Tap worker corrupts non-ASCII content → also splits sessions ✓
`lib/tap-worker.js:57,271`
The worker loops `p.feed(bufs[i])` and `feed()` does `Buffer.from(buf).toString('utf8')` **per buffer**. A multibyte char split across two batch buffers emits U+FFFD on both sides, corrupting the SSE scan, `responseContent`, and char/token counts. Since `storeStateKey` keys on `responseContent`, this is **another session-split cause for any non-ASCII conversation**. Client bytes are unaffected (separate pipe).
**Fix:** `p.feed(Buffer.concat(bufs))` once, or a streaming `TextDecoder`.

### A3. OpenAI `"usage"` substring drops content ✓
`lib/tap-worker.js:116-128`
A content-bearing SSE line containing the literal `"usage"` substring is JSON.parsed, then unconditionally `return`s at line 128 — even when `chunk.usage` is absent — skipping the content scan. Content/chars/responseContent for that delta are silently lost.
**Fix:** move the `return` inside the `if (u && …)` block.

### A4. Stale upstream `concurrent_sessions` starves admission after a burst ✓
`lib/concurrency.js:225`
`canStart` uses `Math.max(activeRequests, effective.concurrent)`. `effective.concurrent` is the upstream figure (includes our own sessions), cached 10s. When our requests release, `activeRequests` drops but `concurrent` stays stale-high → `known` stays ≥ limit → new requests stall ~10s after any burst drains. `releaseThrottleSlot` calls `refreshUsageSoon()` (a no-op while fresh) then wakes waiters *before* the fetch resolves.
**Fix:** on release with waiters queued, force a freshness-bypassing refresh; or decay `concurrent` toward `activeRequests`.

### A5. Hot-reload doesn't re-arm the broadcast timer → stale snapshots ✓
`lib/server.js:43` + `lib/sessions.js:251`
Reload calls `scheduleSessionsBroadcast()`, but its `if (!state.sessionsBroadcastTimer)` guard skips when sessions are active, so the **old** (purged-from-cache, closure-retained) `sessions.js` keeps emitting stale-format snapshots until sessions go idle.
**Fix:** `clearInterval` + null the timer before re-arming in the reload path.

### A6. WebSocket pongs are never sent ✓
`lib/handlers.js:255`
The comment "pings auto-answered by Node's net" is false — Node's net doesn't handle WS-layer pings. Incoming ping frames are dropped. RFC §5.5.2 non-conformance; any client that pings for keepalive times out. Likely dormant today (browser dashboard doesn't ping), but blocks the keepalive fix in C1.
**Fix:** on `frame.type === 0x09`, write a pong with the same payload.

### A7. WebSocket frame parser: RFC gaps + O(n²) dribble-DoS ✓
`lib/ws.js:80,83`, `lib/handlers.js:248`
(1) unmasked client frames accepted (§5.1 violation — must close 1002); (2) control frames (ping/pong/close) not capped to 125 bytes and not checked FIN=1; (3) `buf = Buffer.concat([buf, chunk])` per chunk is O(n²) — a client dribbling a 1 MiB frame as 1-byte writes forces ~512 GB of copying.
**Fix:** reject unmasked; enforce ≤125 + FIN for control opcodes; replace concat-per-chunk with a single growing buffer capped at `MAX_FRAME_SIZE`.

---

## B. Security risks

### B1. Cross-site WebSocket hijack on `/ws` ✓
`lib/handlers.js:236`, `lib/server.js:116-120`
The `/ws` upgrade is **never authenticated** and does no Origin check, regardless of `proxyAuthEnabled`. Any web page in the user's browser can `new WebSocket('ws://localhost:8084/ws')` and receive live session data (prompts, assistant text, group keys, token totals). Bypasses proxy auth even when enabled.
**Fix:** `authorized(req, url)` (or an Origin allowlist) before `ws.handshake`; require `Sec-WebSocket-Version: 13`.

### B2. Auth fail-open on corrupted config ✓
`lib/config.js:17-18` + `lib/auth.js:40`
Bad JSON → `readJSON` returns `{}` → `proxyApiKeys:[]` → auth silently disabled, process keeps serving. Combined with B3 this is a remote auth-bypass.
**Fix:** on parse failure of an *existing* config file, refuse to boot or retain the prior in-memory config and log loudly.

### B3. No non-loopback bind guard ✓
`lib/server.js:138`
`LISTEN_ADDR=0.0.0.0:…` with empty `API_KEYS` exposes every admin endpoint (`/api/shutdown`, `/api/clear-state` remote state-wipe, `/api/reload`) plus an open upstream relay burning the UMANS key. README warns in prose; code doesn't enforce.
**Fix:** in `boot()`, refuse (or require an explicit flag) when host ≠ loopback and `proxyApiKeys` is empty.

### B4. POST /api/config can inject/overwrite the UMANS key when auth disabled ✓
`lib/handlers.js:104-106`
Credential injection + exfiltration vector, no audit log.
**Fix:** require proxyAuth (or loopback) for the mutation path unconditionally.

### B5. Debug/admin endpoints expose data when auth disabled ✓
`lib/handlers.js:299,219-234`
`/api/debug/coalesce` (group keys, content previews) and `/api/system/info` (pid, listenAddr, upstream, node version) are gated only by the optional proxy key. `pid` disclosure aids PID-confusion on loopback.
**Fix:** treat as always-auth (or always-loopback).

### B6. Proxy key accepted as `?key=` query param ✓
`lib/auth.js:41`
Lands in access logs, browser history, reverse-proxy/referer.
**Fix:** header-only (`x-api-key`/`Authorization`); drop the query path.

---

## C. Reliability & resource-leak risks
> **Status: C1–C10 FIXED** (uncommitted) — keepalive sweep+pong-timeout (C1), cold-start floor `usageEverFetched` (C2), stale-concurrent collapse (C3), inFlightControllers registered before queue wait (C4), no-wake on 429 (C5), mid-stream SSE error frame via pipe restructure so `res` survives body errors (C6), `safeHeaders` strips content-encoding only for decoded encodings + upstream `Accept-Encoding` restriction (C7), `tapActiveTaps` re-init on worker restart (C8), `closeAllConnections`+retry margin (C9), process-level handlers + `ws.send` try/catch (C10). +13 tests (162 total).

### C1. Half-open WS connections leak forever ✓
`lib/ws.js:110,140`
`socket.setTimeout(0)` + dead `ws.ping()` (zero call sites) → dead clients accumulate in `state.wsClients`, still receiving broadcasts, leaking sockets/memory.
**Fix:** wire a periodic ping sweep + pong-timeout close (this and A6 together fix WS liveness).

### C2. Cold-start fail-open (unthrottled at boot) ✓
`lib/concurrency.js:224`
`limit == null` admits all until the first `/usage` succeeds; if that fetch fails the proxy runs unthrottled → account-wide 429/ban risk.
**Fix:** fail closed (or to a configured safety floor) until the first successful fetch.

### C3. /usage outage → fail-closed starvation ✓
`lib/concurrency.js:177`
On fetch failure `concurrencyCache` keeps the last-good `concurrent`; if that was at/over the limit, all new requests block for the whole outage.
**Fix:** when cache is stale and `concurrent > activeRequests`, prefer `activeRequests`.

### C4. Queued waiters not aborted on shutdown ✓
`lib/chat.js:95` + `lib/server.js:61`
`inFlightControllers.add` runs *after* acquire, so queued waiters aren't in the set → shutdown doesn't abort them; they hang until the 15m `requestTimeout` or the 5s force-exit.
**Fix:** move the add before the queue wait, or iterate `throttleWaiters` on shutdown.

### C5. 429 cascade re-arms the 72m cooldown ✓
`lib/concurrency.js:219`
`notifyUpstream429` immediately wakes waiters below soft, which may also 429, sliding the window repeatedly.
**Fix:** skip waking on 429 (let in-flight drain), or add a short hold-off.

### C6. Mid-stream upstream error masked as silent truncation ✓
`lib/chat.js:163,170-175`
A non-abort pipe error after headers are sent → silent `res.end()`, no SSE error frame → clients can't distinguish abort from real failure and may accept partial output as complete.
**Fix:** emit a terminal SSE error frame (OpenAI `data: {error}…[DONE]`; Anthropic `event: error`) before ending; reserve silent-end for abort/closed only.

### C7. `content-encoding` stripped for encodings undici doesn't decode ✓
`lib/auth.js:25`
`safeHeaders` strips `content-encoding` unconditionally. For gzip/br undici auto-decodes (safe), but for zstd (Node <22) or unknown encodings the client gets compressed bytes with no header → silent body corruption.
**Fix:** only strip when actually decoded, or pass through verbatim (the pipe forwards bytes unmodified).

### C8. Worker restart loses in-flight tap parsers ✓
`lib/chat-tap.js:116`
`init` is posted only in the constructor. After death + restart, in-flight taps' chunks hit `parsers.get(id) → undefined → return`, so those sessions lose all char/token/responseContent for the rest of the stream.
**Fix:** on restart, re-`init` for all still-active tap ids (track them on the main thread).

### C9. Restart can leave no proxy running ✓
`lib/handlers.js:163-171`
Successor spawned immediately; parent `server.close()` is async. If the parent doesn't release the port within the 20s retry window, the successor gives up → service down.
**Fix:** wait for the parent's `close` callback (or `closeAllConnections`) before spawning, or extend successor retries.

### C10. No process-level `unhandledRejection`/`uncaughtException` ✓
Only the worker has one. A throw from a timer callback (notably `broadcastEvent → ws.send`, the one socket-write path *without* try/catch — `lib/ws.js:136`) escapes and, per Node 15+ default, can crash the proxy mid-traffic.
**Fix:** add top-level handlers that log+continue; wrap `ws.send`'s write in try/catch matching `close()`/`ping()`.

---

## D. Latent (unreachable today, fragile — cheap to harden)
> **Status: D1 FIXED** — `if (woken) state.activeRequests--` guards both abort paths (commit c8e580a) + regression test "woken-then-aborted slot".

### D1. `activeRequests` leak on woken-then-abort ✓
`lib/concurrency.js:261,270` + `lib/chat.js:91`
Unreachable today: both abort sources (timeout, res-close) are macrotasks and microtasks drain before the woken awaiter resumes. But if any future change calls `controller.abort()` synchronously in the wake stack, `activeRequests` leaks permanently → progressive starvation.
**Fix:** defensively `if (woken) state.activeRequests--` before throwing at 261/270. Insurance, not a live bug.

---

## E. Smells & consistency (condensed)
> **Status: E FIXED** (uncommitted) — swallowed catches logged (cleanup-critical only); requiresProxyAuth/router parity test; reload re-reads config.json (non-fatal, rollback); REQUEST_TIMEOUT 1s clamp + IPv6 bracket strip; dead keepalive removed; refreshUsageTimer .unref() + cadence aligned; fetchUmansUsage in-flight dedup; double-finalize guard + TTL leak fix + O(group) eviction index (sessionsByGroup); _scanStringLen indexOf fast path; json-stream done mid-buffer check; snapReasoningLevel docstring; dashboard grouped-card tps sums only active sessions (in dashboard.html, uncommitted with the user's WIP). extractThrottle/getEffectiveConcurrency divergence was already fixed externally.

- **Swallowed catches hide failures** ✓ — `chat.js:181-183,104`, `chat-tap.js:184`, `tap-worker.js:113,117` all `catch {}`. Makes throttle/coalesce/telemetry degradation invisible. Log at debug without changing control flow.
- **`requiresProxyAuth` and the router are parallel hand-synced lists** ✓ `auth.js:48` vs `handlers.js:263` — adding a route and forgetting the auth list = silent bypass, no test. Derive from one source or assert parity in tests.
- **Reload doesn't re-read `config.json`** ✓ `server.js:69` — external edits invisible until restart; and `process.exit(1)` inside the reload path (`server.js:88`) makes a parse error fatal to a supposed-to-be-non-fatal reload.
- **`extractThrottle` vs `getEffectiveConcurrency` divergence** ✓ `concurrency.js:135` vs `:206` — during a 429 cooldown, `/usage` reports the hard limit while admissions clamp to soft → dashboard misleads operators about why requests queue.
- **unitless `REQUEST_TIMEOUT` = milliseconds** ✓ `config.js:36` — `REQUEST_TIMEOUT=30` → 30ms, aborts everything; no upper bound.
- **IPv6 listen address broken** ✓ `config.js:82` — `[::1]:8084` keeps brackets; `server.listen` fails.
- **Dead `keepalive` option** ✓ `concurrency.js:244` — sole caller passes none; branch + `clearInterval` dead.
- **`refreshUsageTimer` not `.unref()`'d** ✓ `concurrency.js:186` — pins the loop; comment says 5s cadence, guard enforces 10s.
- **`fetchUmansUsage` no in-flight dedup for direct callers** ✓ `concurrency.js:237` — `handleUsage`/`handleConcurrency`/cold-acquire can fire simultaneous `/usage` requests.
- **Double-finalize guard missing** ✓ `sessions.js:89` — no `if (session.endedAt) return`; a double-call double-counts the group summary and leaks a 5min timer.
- **Dead TTL condition** ✓ `sessions.js:74-76` — `Date.now()-lastSeenAt >= TTL` is always true when the timer fires.
- **O(n) eviction scan per cache-miss finalize** ✓ `sessions.js:103-107` — scans all sessions; add a `groupKey→Set<id>` index.
- **`_scanStringLen` per-char loop** ✓ `tap-worker.js:149` — dominant worker cost; mirror `json-stream.js`'s indexOf fast path.
- **`json-stream` `done` not checked mid-buffer** ✓ `json-stream.js:115` — wasted parse; n>1 concatenates choices into the coalescing key.
- **`snapReasoningLevel` docstring mismatch** ✓ `reasoning.js:53` vs `chat.js:97` — null means "drop," not "leave untouched."
- **Dashboard grouped-card tps sums finished sessions** ✓ `dashboard.html:1248` — inflates current-throughput display.

---

## F. Test gaps (high-value, consolidated)
> **Status: F partial** — added: finalizeSession idempotency, projectStatus (upstream.js first tests), requiresProxyAuth/router parity, IPv6/timeout, + the C1/C2/C3/C5/C6/C8/C10 tests, D1 regression. Still open: store-only-on-done coalesce gate, worker death/restart degrade, hot-reload E2E.

- `finalizeSession` (finalTps, eviction, TTL, groupSummary).
- The **store-only-on-`done` coalesce gate** (`chat.js:104`) — abort/error must not write `stateMap`; directly guards the assistant-flatten split fix.
- Worker death/restart graceful degrade (`onEnd` resolves null, restart after `RESTART_MS`).
- **`lib/upstream.js` has zero tests** — `projectStatus` is pure, trivial to test.
- WS `parseFrame`/pong/fragmentation/tooLarge.
- Concurrency-under-abort (queue-wait abort + `activeRequests` accounting across every exit path).
- Hot reload (cache invalidation, state persistence, rollback on require-throw).
- `requiresProxyAuth`/router parity.
- Corrupted-config fail-open.
- The latent-leak path (D1) has no regression guard.

---

## G. Docs
> **Status: G FIXED** — README line 84 (worker thread, not setImmediate), line 98 (5 minutes, not 5 seconds), B3 enforcement note (refuses to boot), reload config re-read note.

README is stale on two points:
- line 84 says telemetry runs in a `setImmediate` drain (it's a worker thread);
- line 98 says completed sessions "remain visible for 5 seconds" (`SESSION_TTL_MS` is 5 **minutes**, `state.js:26`).

---

## Recommended order

Each batch is independently shippable and testable.

1. **Correctness bugs that affect users daily:** A1 (TPS), A2 (UTF-8 split — also a split cause), A3 (usage substring), A4 (burst starvation). Plus fix the TPS test that bakes in the bug.
2. **Security:** B1 (CSWSH), B2 (config fail-open), B3 (non-loopback guard), B6 (query-key) — small and high-value.
3. **WS liveness bundle:** A6 + A7 + C1 together (pongs, frame hardening, keepalive pings) + a `parseFrame` test.
4. **Reliability:** C2/C3 (fail-open/closed), C4 (shutdown abort), C6 (SSE error frame), C10 (global handlers + `ws.send` try/catch).
5. **Smells + test gaps** as cleanup.
