'use strict';

const state = require('./state');
const { readBody, openAIError, anthropicError, logError } = require('./http');
const { authHeaders, safeHeaders, upstreamURL } = require('./auth');
const { resolveGroupKey, storeStateKey } = require('./coalesce');
const { createSession, finalizeSession, scheduleSessionsBroadcast, broadcastEvent } = require('./sessions');
const { ChatTap } = require('./chat-tap');
const { acquireThrottleSlot, releaseThrottleSlot, refreshUsageSoon } = require('./concurrency');
const { fetchModelInfo } = require('./upstream');
const { snapReasoningLevel } = require('./reasoning');

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
  const rateLimitType = isAnthropic ? 'rate_limit_error' : 'rate_limit_error';
  const serverErrorType = isAnthropic ? 'api_error' : 'server_error';
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

  // OpenAI only: snap reasoning_effort to a level the model actually supports.
  if (!isAnthropic && payload.reasoning_effort != null) {
    await fetchModelInfo();
    const snapped = snapReasoningLevel(payload.model, payload.reasoning_effort);
    if (snapped == null) { delete payload.reasoning_effort; mutated = true; }
    else if (snapped !== payload.reasoning_effort) { payload.reasoning_effort = snapped; mutated = true; }
  }

  const controller = new AbortController();
  let closed = res.destroyed;
  let sessionStatus = 'done';
  const onClose = () => { closed = true; sessionStatus = 'aborted'; controller.abort(); };
  if (closed) { sessionStatus = 'aborted'; controller.abort(); }
  else res.on('close', onClose);

  // For streaming requests, send 200 + SSE headers BEFORE acquiring the
  // throttle slot. This lets omp's fetch resolve, starts its SSE idle
  // watchdog, and lets us emit comment-frame keepalives while queued.
  const isStream = !!payload.stream;
  let headersSentEarly = false;
  if (isStream && !closed) {
    // Disable Nagle's algorithm so small SSE chunks are sent immediately
    // instead of waiting for the delayed-ACK timer (up to 40ms per chunk).
    try { res.socket?.setNoDelay(true); } catch {}
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    headersSentEarly = true;
  }

  // Helper: write an SSE error event in the appropriate shape.
  const writeSseError = (message, type) => {
    if (isAnthropic) {
      try { res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type, message } })}\n\n`); } catch {}
    } else {
      try { res.write(`data: ${JSON.stringify({ error: { message, type } })}\n\n`); } catch {}
    }
  };

  // Start the request timeout BEFORE acquiring the throttle slot so it
  // bounds total wall time (queue wait + upstream), not just the upstream
  // phase. Aborting the controller during queue wait unblocks the slot.
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; sessionStatus = 'aborted'; controller.abort(); }, state.config.requestTimeout);

  try {
    await acquireThrottleSlot(res, controller.signal, {
      keepalive: headersSentEarly ? () => {
        if (!closed && !res.destroyed) {
          try { res.write(': queued\n\n'); } catch {}
        }
      } : undefined,
    });
  } catch (err) {
    clearTimeout(timeout);
    if (closed) return;
    if (headersSentEarly) {
      writeSseError(err.message, rateLimitType);
      res.end();
    } else {
      return errorFn(res, 503, err.message, rateLimitType);
    }
    return;
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

    // OpenAI only: inject stream_options.include_usage to get exact token
    // counts on the final stream chunk. Only re-serialize if this changes.
    if (!isAnthropic && payload.stream && !payload.stream_options?.include_usage) {
      payload.stream_options = { ...(payload.stream_options || {}), include_usage: true };
      mutated = true;
    }

    // Forward raw bytes unless we mutated the payload.
    const body = mutated ? JSON.stringify(payload) : rawBody;
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

    if (headersSentEarly) {
      if (!upstream.ok) {
        const errText = await upstream.text().catch(() => upstream.statusText);
        writeSseError(`upstream ${upstream.status}: ${errText.slice(0, 500)}`, 'upstream_error');
        res.end();
        sessionStatus = 'error';
        return;
      }
    } else {
      res.writeHead(upstream.status, safeHeaders(upstream.headers));
    }
    if (!upstream.body) return res.end();

    const reader = upstream.body.getReader();
    try {
      while (!closed) {
        const { done, value } = await reader.read();
        if (done) break;
        tap.onChunk(value);
        if (controller.signal.aborted) break;
        // Fast path: res.write returns true when the kernel accepts the
        // bytes synchronously. Skip the await — a Promise.resolve() here
        // would force a microtask checkpoint between every chunk, adding
        // latency when multiple SSE chunks arrive in one kernel read.
        // Only await when the socket is backpressured and we must wait
        // for 'drain'.
        if (!res.write(value)) {
          await new Promise((resolve, reject) => {
            const cleanup = () => {
              res.off('drain', onDrain);
              res.off('close', onClose);
              res.off('error', onError);
              controller.signal.removeEventListener('abort', onAbort);
            };
            const onDrain = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); resolve(); };
            const onError = (err) => { cleanup(); reject(err); };
            const onAbort = () => { cleanup(); reject(new Error('aborted')); };
            res.once('drain', onDrain);
            res.once('close', onClose);
            res.once('error', onError);
            controller.signal.addEventListener('abort', onAbort, { once: true });
          });
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
    if (!closed) res.end();
  } catch (err) {
    if (!closed) logError(isAnthropic ? 'Messages proxy failed' : 'Chat proxy failed', err);
    if (!res.headersSent && !headersSentEarly && !closed) {
      errorFn(res, timedOut ? 504 : 502, timedOut ? 'upstream request timed out' : err.message, errorType);
    } else if (!closed) {
      if (headersSentEarly) writeSseError(timedOut ? 'upstream request timed out' : err.message, serverErrorType);
      res.end();
    }
    sessionStatus = closed || timedOut ? 'aborted' : 'error';
  } finally {
    clearTimeout(timeout);
    res.off('close', onClose);
    try { releaseThrottleSlot(); } catch {}
    try { if (tap) tap.onEnd(); } catch {}
    try { finalizeSession(session, sessionStatus); } catch {}
    state.inFlightControllers.delete(controller);
  }
}

module.exports = { proxyRequest };
