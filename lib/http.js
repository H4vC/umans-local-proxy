'use strict';

const state = require('./state');

function writeJSON(res, status, body, headers = {}) {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { ...headers, 'Content-Type': 'application/json', 'Content-Length': data.length });
  res.end(data);
}

function writeText(res, status, body, contentType = 'text/plain; charset=utf-8', headers = {}) {
  const data = Buffer.from(body);
  res.writeHead(status, { ...headers, 'Content-Type': contentType, 'Content-Length': data.length });
  res.end(data);
}

function abortError(signal) {
  const reason = signal?.reason;
  const err = reason instanceof Error ? reason : new Error('request aborted');
  err.name = 'AbortError';
  return err;
}

function readBody(req, { signal } = {}) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;
    let settled = false;
    const finish = (fn) => {
      if (settled) return;
      settled = true;
      req.off('close', onClose);
      signal?.removeEventListener?.('abort', onAbort);
      fn();
    };
    const onClose = () => finish(() => reject(new Error('client disconnected')));
    const onAbort = () => {
      // Keep the connection drainable so a timeout can still return an HTTP
      // error, but stop retaining any more of the inbound payload.
      try { req.resume?.(); } catch {}
      finish(() => reject(abortError(signal)));
    };
    if (signal?.aborted) { onAbort(); return; }
    signal?.addEventListener?.('abort', onAbort, { once: true });
    req.on('data', (chunk) => {
      if (settled || tooLarge) return;
      size += chunk.length;
      if (size > state.MAX_BODY_SIZE) {
        tooLarge = true;
        const err = new Error('request body too large');
        err.statusCode = 413;
        finish(() => reject(err));
        // Stop consuming an oversized upload immediately; otherwise a client
        // can keep the socket busy after we've already selected the 413 path.
        if (typeof req.destroy === 'function') req.destroy(err);
        else if (typeof req.pause === 'function') req.pause();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => finish(() => resolve(Buffer.concat(chunks, size).toString('utf8'))));
    req.on('error', (err) => finish(() => reject(err)));
    req.on('close', onClose);
  });
}


function openAIError(res, status, message, type = 'server_error') {
  writeJSON(res, status, { error: { message, type, param: null, code: null } });
}

function anthropicError(res, status, message, type = 'invalid_request_error') {
  writeJSON(res, status, { type: 'error', error: { type, message } });
}

function logError(context, err) {
  const detail = err == null ? '' : (typeof err === 'string' ? err : (err?.message || String(err)));
  console.error(detail ? `${context}: ${detail}` : context);
}

module.exports = {
  writeJSON,
  writeText,
  readBody,
  openAIError,
  anthropicError,
  logError,
};
