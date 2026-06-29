'use strict';

const state = require('./state');
const { readBody, openAIError, anthropicError, logError } = require('./http');
const { authHeaders, safeHeaders, upstreamURL } = require('./auth');
const { resolveGroupKey, storeStateKey } = require('./coalesce');
const { createSession, finalizeSession, scheduleSessionsBroadcast, broadcastEvent } = require('./sessions');
const { ChatTap, createTapStream } = require('./chat-tap');
const { acquireThrottleSlot, releaseThrottleSlot, refreshUsageSoon, notifyUpstream429 } = require('./concurrency');
const { fetchModelInfo } = require('./upstream');
const { snapReasoningLevel } = require('./reasoning');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

// Unified chat proxy for both OpenAI (/chat/completions) and Anthropic
// (/messages) shapes. The `shape` option controls:
//   - Error format (openAIError vs anthropicError + SSE error event shape)
//   - Auth headers (Bearer vs x-api-key + anthropic-version)
//   - Reasoning snapping (OpenAI only)
//   - stream_options.include_usage injection (OpenAI only)
//   - Upstream path
//   - ChatTap parse shape
async function proxyRequest(req, res, { shape } = {}) {
  const isAnthropic = shape === 'anthropic';
  const errorFn = isAnthropic ? anthropicError : openAIError;
  const errorType = isAnthropic ? 'api_error' : 'invalid_request_error';
  const upstreamPath = isAnthropic ? '/messages' : '/chat/completions';

  if (req.method !== 'POST') return errorFn(res, 405, 'method not allowed', 'invalid_request_error');
  if (!state.config.apiKey) return errorFn(res, 400, 'UMANS API key is not configured', 'invalid_request_error');

  let rawBody;
  try { rawBody = await readBody(req); }
  catch (err) { return errorFn(res, err.statusCode || 400, err.message, 'invalid_request_error'); }

  let payload;
  try { payload = JSON.parse(rawBody || '{}'); }
  catch { return errorFn(res, 400, 'request body must be valid JSON', 'invalid_request_error'); }

  if (!payload.model) return errorFn(res, 400, 'model is required', 'invalid_request_error');
  if (state.config.enabledModels.length && !state.config.enabledModels.includes(payload.model)) {
    return errorFn(res, 400, `model is not enabled: ${payload.model}`, 'invalid_request_error');
  }

  // Track whether we mutate the payload. If we don't, we forward the raw
  // bytes as-is — no re-serialization of a potentially 100K+ token prompt.
  let mutated = false;

  const controller = new AbortController();
  let closed = res.destroyed;
  let sessionStatus = 'done';
  // 'close' fires on BOTH normal completion and premature client disconnect.
  // Only the latter is an abort: if the response was fully sent (writableEnded),
  // the session completed normally. (The pipe path awaits res completion, so
  // 'close' can fire before finalizeSession — unlike the old manual loop.)
  const onClose = () => { if (res.writableEnded) return; closed = true; sessionStatus = 'aborted'; controller.abort(); };
  if (closed) { sessionStatus = 'aborted'; controller.abort(); }
  else res.on('close', onClose);

  // For streaming requests, delay response headers until the upstream responds.
  // NEVER send HTTP 200 before upstream status is known: a real upstream 429/403
  // would become a 200 SSE error body, and OMP would not surface the rate limit.
  if (payload.stream && !closed) {
    // Disable Nagle's algorithm so small SSE chunks are sent immediately
    // instead of waiting for the delayed-ACK timer (up to 40ms per chunk).
    try { res.socket?.setNoDelay(true); } catch {}
  }

  // OpenAI only: snap reasoning_effort to a level the model actually supports.
  // Runs before the upstream request. Because response headers are now delayed
  // until upstream responds, non-2xx statuses still reach clients as HTTP errors.
  if (!isAnthropic && payload.reasoning_effort != null) {
    await fetchModelInfo();
    const snapped = snapReasoningLevel(payload.model, payload.reasoning_effort);
    if (snapped == null) { delete payload.reasoning_effort; mutated = true; }
    else if (snapped !== payload.reasoning_effort) { payload.reasoning_effort = snapped; mutated = true; }
  }


  // Start the request timeout BEFORE acquiring the throttle slot so it
  // bounds total wall time (queue wait + upstream), not just the upstream
  // phase. Aborting the controller during queue wait unblocks the slot.
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; sessionStatus = 'aborted'; controller.abort(); }, state.config.requestTimeout);

  try {
    await acquireThrottleSlot(res, controller.signal);
  } catch (err) {
    clearTimeout(timeout);
    if (closed) return;
    return errorFn(res, 503, err.message, isAnthropic ? 'rate_limit_error' : 'rate_limit_error');
  }

  let session, tap;
  state.inFlightControllers.add(controller);
  try {
    const { groupKey, prefixChain } = resolveGroupKey(payload.model, payload.messages || []);
    session = createSession({ model: payload.model, stream: payload.stream, groupKey });
    tap = new ChatTap(session, {
      stream: payload.stream,
      shape: isAnthropic ? 'anthropic' : 'openai',
      onFinalize: (s) => {
        if (sessionStatus === 'done') {
          try { storeStateKey(payload.model, payload.messages || [], s.responseContent, s.groupKey, prefixChain); } catch {}
        }
        s.responseContent = '';
      },
    });
    broadcastEvent('session', { type: 'start', id: session.id, active: state.activeRequests, queued: state.queuedRequests });
    scheduleSessionsBroadcast();

    // OpenAI streaming: inject stream_options.include_usage to get exact token
    // counts on the final stream chunk. Avoid re-serializing the whole payload
    // (O(prompt) object-graph walk) when we can splice the option into rawBody:
    // for valid top-level JSON the last '}' is provably the top-level closer,
    // so lastIndexOf + a one-copy insert suffices. Fall back to spread+stringify
    // when the snap already mutated payload (its change isn't in rawBody) or
    // stream_options already exists (a second key would collide).
    let body;
    if (!isAnthropic && payload.stream && !payload.stream_options?.include_usage) {
      if (!mutated && !payload.stream_options) {
        const idx = rawBody.lastIndexOf('}');
        body = idx > 0
          ? rawBody.slice(0, idx) + ',"stream_options":{"include_usage":true}' + rawBody.slice(idx)
          : JSON.stringify({ ...payload, stream_options: { include_usage: true } });
      } else {
        payload.stream_options = { ...(payload.stream_options || {}), include_usage: true };
        body = JSON.stringify(payload);
      }
    } else {
      body = mutated ? JSON.stringify(payload) : rawBody;
    }
    const upstream = await fetch(upstreamURL(upstreamPath), {
      method: 'POST',
      headers: authHeaders(
        { 'Content-Type': 'application/json', Accept: payload.stream ? 'text/event-stream' : 'application/json' },
        { anthropic: isAnthropic },
      ),
      body,
      signal: controller.signal,
    });
    refreshUsageSoon();

    if (upstream.status === 429) notifyUpstream429();
    if (!upstream.ok) sessionStatus = 'error';
    res.writeHead(upstream.status, safeHeaders(upstream.headers));
    if (!upstream.body) return res.end();

    // Native pipe: upstream body -> tap Transform -> client response. Bytes
    // ride stream.pipeline's optimized path (native backpressure, no per-chunk
    // drain-promise dance); the tap Transform does only O(1) byte counting and
    // enqueues the deferred SSE/JSON parse, so telemetry never delays bytes.
    // Abort (timeout or client disconnect) propagates via the controller signal
    // and is treated as a clean break-and-end — no error surfaced to the
    // client on abort, matching the prior manual loop.
    const tapStream = createTapStream(tap);
    try {
      await pipeline(Readable.fromWeb(upstream.body), tapStream, res, {
        signal: controller.signal,
        end: true,
      });
    } catch (err) {
      if (controller.signal.aborted || closed) {
        // Timeout or client disconnect: silent end (sessionStatus already set).
      } else {
        throw err;
      }
    }
    if (!closed && !res.writableEnded && !res.destroyed) res.end();
  } catch (err) {
    if (!closed) logError(isAnthropic ? 'Messages proxy failed' : 'Chat proxy failed', err);
    if (!res.headersSent && !closed) {
      errorFn(res, timedOut ? 504 : 502, timedOut ? 'upstream request timed out' : err.message, errorType);
    } else if (!closed && !res.destroyed) {
      res.end();
    }
    sessionStatus = closed || timedOut ? 'aborted' : 'error';
  } finally {
    clearTimeout(timeout);
    res.off('close', onClose);
    try { releaseThrottleSlot(); } catch {}
    try { if (tap) await tap.onEnd(); } catch {}
    try { finalizeSession(session, sessionStatus); } catch {}
    state.inFlightControllers.delete(controller);
  }
}

module.exports = { proxyRequest };
