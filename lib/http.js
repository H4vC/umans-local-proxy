'use strict';

const state = require('./state');

function writeJSON(res, status, body, headers = {}) {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { ...headers, 'Content-Type': 'application/json', 'Content-Length': data.length });
  res.end(data);
}

function writeText(res, status, body, contentType = 'text/plain; charset=utf-8') {
  const data = Buffer.from(body);
  res.writeHead(status, { 'Content-Type': contentType, 'Content-Length': data.length });
  res.end(data);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;
    let settled = false;
    const finish = (fn) => { if (settled) return; settled = true; req.off('close', onClose); fn(); };
    const onClose = () => finish(() => reject(new Error('client disconnected')));
    req.on('data', (chunk) => {
      if (tooLarge) return;
      size += chunk.length;
      if (size > state.MAX_BODY_SIZE) {
        tooLarge = true;
        const err = new Error('request body too large');
        err.statusCode = 413;
        finish(() => reject(err));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => finish(() => resolve(Buffer.concat(chunks).toString('utf8'))));
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
  console.error(`${context}: ${err?.message || err}`);
}

module.exports = {
  writeJSON,
  writeText,
  readBody,
  openAIError,
  anthropicError,
  logError,
};
