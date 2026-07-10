'use strict';

const state = require('./state');
const { SCALING_DEFAULTS } = require('./config');
const { readBody, openAIError, anthropicError, logError } = require('./http');
const { logRequest } = require('./request-log');
const { authHeaders, safeHeaders, upstreamURL } = require('./auth');
const { resolveGroupKey, storeStateKey } = require('./coalesce');
const { createSession, finalizeSession, scheduleSessionsBroadcast, broadcastEvent } = require('./sessions');
const { ChatTap, createTapStream } = require('./chat-tap');
const { acquireThrottleSlot, releaseThrottleSlot, refreshUsageSoon, notifyUpstream429 } = require('./concurrency');
const { fetchModelInfo } = require('./upstream');
const { snapReasoningLevel } = require('./reasoning');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

// Upstream permits only bound dispatched work. Bound body intake separately so
// slow or oversized uploads cannot consume unbounded heap before admission.
const MAX_CONCURRENT_BODY_READS = SCALING_DEFAULTS.bodyReads;
function bodyReadLimit() {
  const value = Number(state.config?.limits?.bodyReads);
  return Number.isSafeInteger(value) && value >= 0 ? value : MAX_CONCURRENT_BODY_READS;
}

function claimBodyRead() {
  if (typeof state.pendingBodyReads !== 'number') state.pendingBodyReads = 0;
  if (state.pendingBodyReads >= bodyReadLimit()) return false;
  state.pendingBodyReads++;
  return true;
}

function releaseBodyRead() {
  if (state.pendingBodyReads > 0) state.pendingBodyReads--;
}

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

  // Start ownership and the wall-clock deadline before reading a potentially
  // large body. This makes uploads, model metadata, queueing, and upstream work
  // equally abortable on disconnect, timeout, and shutdown.
  const controller = new AbortController();
  let closed = res.destroyed;
  let timedOut = false;
  let sessionStatus = 'done';
  const onClose = () => {
    if (res.writableEnded) return;
    closed = true;
    sessionStatus = 'aborted';
    controller.abort();
  };
  if (closed) controller.abort();
  else res.on('close', onClose);
  const timeout = setTimeout(() => {
    timedOut = true;
    sessionStatus = 'error';
    controller.abort(new Error('request timed out'));
  }, state.config.requestTimeout);
  timeout.unref?.();
  state.inFlightControllers.add(controller);

  let bodyReadClaimed = false;
  let acquired = false;
  let session, tap;
  let payload;
  let tag = 'chat:unparsed';
  try {
    if (closed) return;
    if (!claimBodyRead()) {
      return errorFn(res, 429, 'proxy request-body capacity is saturated', 'rate_limit_error');
    }
    bodyReadClaimed = true;

    let rawBody;
    try {
      rawBody = await readBody(req, { signal: controller.signal });
    } catch (err) {
      if (closed) return;
      if (timedOut) return errorFn(res, 504, 'request timed out while reading body', errorType);
      return errorFn(res, err.statusCode || 400, err.message, 'invalid_request_error');
    } finally {
      releaseBodyRead();
      bodyReadClaimed = false;
    }
    if (controller.signal.aborted) {
      if (!closed && timedOut) return errorFn(res, 504, 'request timed out while reading body', errorType);
      return;
    }

    try { payload = JSON.parse(rawBody || '{}'); }
    catch { return errorFn(res, 400, 'request body must be valid JSON', 'invalid_request_error'); }

    const requestId = ++state.requestSeq;
    tag = isAnthropic ? `chat#${requestId}:anthropic` : `chat#${requestId}:openai`;
    logRequest(tag, 'arrived', { method: req.method, path: req.url, model: payload.model, stream: !!payload.stream });

    if (!payload.model) return errorFn(res, 400, 'model is required', 'invalid_request_error');
    if (state.config.enabledModels.length && !state.config.enabledModels.includes(payload.model)) {
      return errorFn(res, 400, `model is not enabled: ${payload.model}`, 'invalid_request_error');
    }

    // Track whether we mutate the payload. If we don't, forward the raw bytes
    // as-is rather than re-serializing a potentially large prompt.
    let mutated = false;
    if (!isAnthropic && payload.reasoning_effort != null) {
      await fetchModelInfo({ signal: controller.signal });
      if (controller.signal.aborted) {
        if (!closed && timedOut) return errorFn(res, 504, 'request timed out while resolving model capabilities', errorType);
        return;
      }
      const snapped = snapReasoningLevel(payload.model, payload.reasoning_effort);
      if (snapped == null) { delete payload.reasoning_effort; mutated = true; }
      else if (snapped !== payload.reasoning_effort) { payload.reasoning_effort = snapped; mutated = true; }
    }

    // Delay response headers until upstream responds so 429/403 remain real
    // HTTP errors rather than a 200 SSE error body.
    if (payload.stream && !closed) {
      try { res.socket?.setNoDelay(true); } catch {}
    }

    try {
      logRequest(tag, 'waiting for concurrency slot', { active: state.activeRequests, queued: state.queuedRequests });
      await acquireThrottleSlot(res, controller.signal);
      acquired = true;
      logRequest(tag, 'acquired concurrency slot', { active: state.activeRequests, queued: state.queuedRequests });
    } catch (err) {
      if (closed) return;
      if (timedOut) return errorFn(res, 504, 'request timed out waiting for an upstream slot', errorType);
      return errorFn(res, err.code === 'ERR_THROTTLE_QUEUE_FULL' ? 429 : (err.statusCode || 503), err.message, 'rate_limit_error');
    }

    const { groupKey, prefixChain, prefixChainNum } = resolveGroupKey(payload.model, payload.messages || []);
    session = createSession({ model: payload.model, stream: payload.stream, groupKey });
    tap = new ChatTap(session, {
      stream: payload.stream,
      shape: isAnthropic ? 'anthropic' : 'openai',
      onFinalize: (s) => {
        if (sessionStatus === 'done') {
          try { storeStateKey(payload.model, payload.messages || [], s.responseContent, s.groupKey, prefixChain, prefixChainNum); } catch (err) { logError('storeStateKey (coalesce) failed', err); }
        }
        s.responseContent = '';
      },
    });
    logRequest(tag, 'passing request upstream', { upstreamPath, stream: !!payload.stream, mutated, model: payload.model });
    broadcastEvent('session', { type: 'start', id: session.id, active: state.activeRequests, queued: state.queuedRequests });
    scheduleSessionsBroadcast();

    // OpenAI streaming: inject stream_options.include_usage for exact final
    // token counts, preserving raw body bytes whenever no other mutation occurs.
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
        { 'Content-Type': 'application/json', Accept: payload.stream ? 'text/event-stream' : 'application/json', 'Accept-Encoding': 'gzip, deflate, br', 'X-Umans-Websearch-Provider': state.config.websearchProvider },
        { anthropic: isAnthropic },
      ),
      body,
      signal: controller.signal,
    });
    refreshUsageSoon();

    if (upstream.status === 429) notifyUpstream429(upstream.headers.get('retry-after'));
    if (!upstream.ok) sessionStatus = 'error';
    logRequest(tag, 'upstream responded', { status: upstream.status, ok: upstream.ok, contentType: upstream.headers.get('content-type') || null });
    res.writeHead(upstream.status, safeHeaders(upstream.headers));
    if (!upstream.body) {
      logRequest(tag, 'upstream had no body; ending turn', { status: upstream.status });
      return res.end();
    }

    logRequest(tag, 'streaming upstream turn', { status: upstream.status, stream: !!payload.stream });
    const tapStream = createTapStream(tap);
    tapStream.on('error', () => {});
    tapStream.pipe(res, { end: true });
    try {
      await pipeline(Readable.fromWeb(upstream.body), tapStream, {
        signal: controller.signal,
        end: true,
      });
      logRequest(tag, 'upstream finished streaming turn', { status: upstream.status, sessionStatus });
    } catch (err) {
      if (closed) {
        sessionStatus = 'aborted';
      } else if (timedOut) {
        sessionStatus = 'error';
        if (payload.stream && !res.destroyed && !res.writableEnded) {
          try {
            if (isAnthropic) {
              res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'api_error', message: 'upstream request timed out' } })}\n\n`);
            } else {
              res.write(`data: ${JSON.stringify({ error: { message: 'upstream request timed out', type: 'server_error', param: null, code: 'timeout' } })}\n\n`);
              res.write('data: [DONE]\n\n');
            }
          } catch {}
        } else if (!res.destroyed) {
          // A non-streaming JSON response cannot be completed safely after a
          // deadline: terminate rather than presenting truncated JSON as valid.
          res.destroy(err);
        }
      } else if (controller.signal.aborted) {
        sessionStatus = 'aborted';
      } else {
        sessionStatus = 'error';
        logRequest(tag, 'upstream stream failed mid-turn', { error: err.message || String(err) });
        if (!res.destroyed && !res.writableEnded) {
          try {
            if (isAnthropic) {
              res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'api_error', message: err.message || 'upstream stream error' } })}\n\n`);
            } else {
              res.write(`data: ${JSON.stringify({ error: { message: err.message || 'upstream stream error', type: 'server_error', param: null, code: null } })}\n\n`);
              res.write('data: [DONE]\n\n');
            }
          } catch {}
        }
      }
    }
    if (!closed && !res.writableEnded && !res.destroyed) res.end();
  } catch (err) {
    if (!closed) logError(isAnthropic ? 'Messages proxy failed' : 'Chat proxy failed', err);
    if (!res.headersSent && !closed) {
      errorFn(res, timedOut ? 504 : 502, timedOut ? 'upstream request timed out' : err.message, errorType);
    } else if (!closed && !res.destroyed) {
      res.destroy(err);
    }
    sessionStatus = closed ? 'aborted' : 'error';
  } finally {
    clearTimeout(timeout);
    res.off('close', onClose);
    if (bodyReadClaimed) releaseBodyRead();
    if (acquired) {
      try { releaseThrottleSlot(); } catch (err) { logError('releaseThrottleSlot failed in finally', err); }
    }
    try { if (tap) await tap.onEnd(); } catch (err) { logError('tap.onEnd failed in finally', err); }
    try { finalizeSession(session, sessionStatus); } catch (err) { logError('finalizeSession failed in finally', err); }
    state.inFlightControllers.delete(controller);
    logRequest(tag, 'released concurrency slot and finalized turn', { status: sessionStatus, active: state.activeRequests, queued: state.queuedRequests });
  }
}

module.exports = { proxyRequest };
