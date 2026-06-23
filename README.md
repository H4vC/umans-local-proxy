# UMANS Proxy

Minimal OpenAI- and Anthropic-compatible proxy for [UMANS AI](https://app.umans.ai/offers/code/docs) (upstream `https://api.code.umans.ai/v1`) with UMANS `/usage`-informed concurrency throttling.

## Requirements

- Node.js 18+ (uses global `fetch`, `AbortController`, `AbortSignal.timeout`)

## Run

Interactive launcher:

```bash
node launcher.js
```

Start with saved settings only:

```bash
node launcher.js --start
```

Run the proxy directly:

```bash
node proxy.js
```

Platform launchers: `start.cmd` (Windows) and `start.sh` (POSIX).

Open `http://127.0.0.1:8084` for the dashboard.

## Configure

Use the launcher, dashboard, or create `.config/config.json`:

```json
{
  "LISTEN_ADDR": "127.0.0.1:8084",
  "API_KEY": "sk-your-umans-api-key",
  "ENABLED_MODELS": ["model-id"],
  "API_KEYS": ["optional-proxy-access-key"],
  "REQUEST_TIMEOUT": "15m",
  "OVERRIDE_CONCURRENCY": 0
}
```

`.config/` is gitignored and never committed — it holds your live API key. `UMANS_API_KEY` overrides `API_KEY`. `API_KEYS` or comma-separated env `API_KEYS` protects `/api/config`, `/api/events`, `/api/umans/*`, and `/v1/*`. Proxy auth is **disabled by default** (empty `API_KEYS`): intended for a localhost single-user tool. Set `API_KEYS` before binding to a non-loopback address. `REQUEST_TIMEOUT` accepts `ms`, `s`, `m`, or `h` units. `OVERRIDE_CONCURRENCY=0` uses UMANS `/usage` limits; positive values cap the UMANS burst/soft limit.

Launcher flags:

```bash
node launcher.js --listen=127.0.0.1:8084 --key=sk-... --models=model-a,model-b --proxyKeys=local-secret --timeout=15m --concurrency=0 --start
```

Add `--no-start` to save settings without starting the proxy.

## Reasoning levels

`GET /v1/models` and `GET /v1/models/info` forward the upstream model list enriched with each model's supported `reasoning` levels (from UMANS `/models/info`). On `POST /v1/chat/completions`, a `reasoning_effort` the requested model does not support is snapped to the nearest supported level, preferring **up** so a max-effort intent never silently downgrades. Common aliases are mapped: `xhi`/`xhigh` → `max`. Requests to disable reasoning on a model that can't disable it (e.g. `umans-kimi-k2.7`) have the field dropped rather than erroring.


## Backend shapes

Both API shapes are always live as passthrough routes — point your client at whichever it speaks:

- **OpenAI**: `POST /v1/chat/completions` with `Authorization: Bearer <key>`. The proxy snaps `reasoning_effort` to the model's supported levels.
- **Anthropic**: `POST /v1/messages` with `x-api-key: <key>` and `anthropic-version: 2023-06-01`. The `thinking` object is forwarded verbatim (UMANS normalizes it server-side).

Throttling, session tracking, and live tok/s telemetry apply to both shapes. Claude Code (`ANTHROPIC_BASE_URL=http://127.0.0.1:8084`) and OpenAI-compatible clients (omp, Cursor, OpenCode) can both use the same proxy simultaneously.
## Throttling and usage

Before chat requests, the proxy reads the concurrency limit from UMANS `/usage` (`limits.concurrency.limit`) and queues requests when **local** in-flight requests reach that limit. Queued requests re-read the effective limit each iteration, so config changes or usage refreshes are honored without restarting. `OVERRIDE_CONCURRENCY` caps below the upstream limit. The upstream-reported `concurrent_sessions` count is shown for insight but does not gate the proxy — only locally-tracked requests do, so other clients on the same key don't throttle you.

`/usage` is cached for 10 seconds. If `/usage` is unavailable or no limit is known, the proxy proceeds without gating rather than blocking chat. The proxy refreshes its cache when any proxied chat request starts and ends.

The dashboard connects to `/api/events` (Server-Sent Events) for live updates — the proxy pushes fresh usage data whenever it fetches `/usage` (on session start/end, manual refresh, config changes). If the SSE stream drops, the dashboard reconnects after 5s (no separate polling loop). The dashboard also force-refreshes before and after a smoke test, and exposes local `active`/`queued` counts. A manual **Refresh now** button force-bypasses the cache.

## Sessions and live TPS

The proxy tracks each in-flight chat request as a session and computes live tokens-per-second (tok/s) without mutating the bytes forwarded to the client. A read-only `ChatTap` observes the upstream stream asynchronously — `onChunk` is O(1) (pushes to a queue), and all JSON parsing / char scanning runs in a `setImmediate` drain between I/O ticks, so telemetry never delays bytes reaching the client.

Token counting: when the upstream response includes a `usage` block (non-streaming responses, or streaming responses with `stream_options: { include_usage: true }`), the exact token count is used. Otherwise, output tokens are estimated from content/reasoning chars (~4 chars/token) and marked as estimated. Per-session token breakdown shows input cached, input uncached, and output tokens, plus a cache hit rate derived from `prompt_tokens_details.cached_tokens`.

- Aggregate tok/s across all sessions (rolling 5s window)
- Per-model tok/s chips (only models actually used)
- Median and p10 (low) tok/s across tracked sessions

The dashboard **Sessions** tab shows per-session cards with:
- Model, status (active/done/aborted/error), stream/sync mode
- Live tok/s (exact or estimated)
- Token breakdown: input cached, input uncached, output, cache hit rate
- Elapsed time, bytes forwarded, session id

Sessions are pushed via the `/api/events` SSE stream (event: `sessions`) at most once per second while sessions are active. Completed sessions remain visible for 5 seconds before being dropped. A local timer in the dashboard updates elapsed times at 2fps between SSE pushes.

## API

- `GET /health`
- `GET /api/umans/usage`
- `GET /api/umans/concurrency`
- `GET /api/umans/sessions` (live TPS + per-session tracking)
- `GET /v1/models/info` (upstream per-model capabilities)
- `GET /v1/models/:id`
- `POST /v1/chat/completions` (OpenAI shape; snaps `reasoning_effort` to supported levels)
- `POST /v1/messages` (Anthropic shape; `x-api-key` + `anthropic-version`)
- `GET /api/config`
- `POST /api/config`

No dependencies; run with Node.js 18+.

## License

[WTFPL](./LICENSE) — do what you want.
