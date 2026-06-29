'use strict';

// Minimal WebSocket server (RFC 6455) — no external deps.
// Supports: handshake, text frames, ping/pong, close, and backpressure-safe
// broadcast. Binary frames are not needed (all payloads are JSON strings).

const crypto = require('crypto');
const state = require('./state');

// ---- Handshake ----
function handshake(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return false; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${accept}`,
    '', '',
  ].join('\r\n');
  socket.write(headers);
  return true;
}

// ---- Frame encoding ----
function encodeFrame(payload, opcode = 0x01) {
  // payload is a string; we encode as a text frame (opcode 0x01).
  const data = Buffer.from(payload, 'utf8');
  const len = data.length;
  let header;
  if (len < 126) {
    header = Buffer.allocUnsafe(2);
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.allocUnsafe(4);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = 0x80 | opcode; // FIN + opcode
  return Buffer.concat([header, data]);
}

function encodePing() {
  const header = Buffer.allocUnsafe(2);
  header[0] = 0x89; // FIN + ping
  header[1] = 0;
  return header;
}

function encodePong(data) {
  const len = data ? data.length : 0;
  const header = Buffer.allocUnsafe(2);
  header[0] = 0x8A; // FIN + pong
  header[1] = len;  // control-frame payloads are always ≤125 (enforced in parseFrame)
  return len ? Buffer.concat([header, data]) : header;
}

// Respond to a client ping (RFC 6455 §5.5.2). Small control frame, written
// directly without the backpressure queue.
function pong(client, data) {
  if (!client) return;
  try { client.socket.write(encodePong(data)); } catch {}
}

// ---- Frame parsing ----
// Returns { type, data, consumed } or null if the frame is incomplete.
//   type: numeric opcode (0x01 text, 0x08 close, 0x09 ping, 0x0A pong,
//         0x00 continuation) | 'tooLarge' (payload > MAX_FRAME_SIZE) |
//         'protocol' (RFC violation: unmasked, or a non-FIN/oversized control frame)
// Both error types let the caller destroy the connection.
const MAX_FRAME_SIZE = 1024 * 1024; // 1 MiB — all payloads are JSON, never large

function parseFrame(buf) {
  if (buf.length < 2) return null;
  const b0 = buf[0];
  const b1 = buf[1];
  const opcode = b0 & 0x0f;
  const fin = b0 & 0x80;
  const masked = b1 & 0x80;
  // RFC 6455 §5.1: frames client→server MUST be masked.
  if (!masked) return { type: 'protocol' };
  let payloadLen = b1 & 0x7f;
  let offset = 2;

  if (payloadLen === 126) {
    if (buf.length < 4) return null;
    payloadLen = buf.readUInt16BE(2);
    offset = 4;
  } else if (payloadLen === 127) {
    if (buf.length < 10) return null;
    payloadLen = Number(buf.readBigUInt64BE(2));
    offset = 10;
  }

  // RFC 6455 §5.5: control frames (opcode >= 0x8) MUST be unfragmented (FIN=1)
  // and carry ≤125 payload bytes.
  if (opcode >= 0x8 && (!fin || payloadLen > 125)) return { type: 'protocol' };

  if (payloadLen > MAX_FRAME_SIZE) return { type: 'tooLarge' };

  if (buf.length < offset + 4) return null; // need the masking key
  const mask = buf.subarray(offset, offset + 4);
  offset += 4;

  if (buf.length < offset + payloadLen) return null;

  const data = Buffer.allocUnsafe(payloadLen);
  for (let i = 0; i < payloadLen; i++) data[i] = buf[offset + i] ^ mask[i & 3];

  return { type: opcode, data, consumed: offset + payloadLen };
}

// ---- Client management ----
// Each WS client is a plain object:
//   { socket, queue: [], paused: false, alive: true, awaitingPong: false }
// `queue` holds frames buffered during backpressure. `paused` is set when
// socket.write returns false. On 'drain', the queue is flushed.

function addClient(socket) {
  const client = { socket, queue: [], paused: false, alive: true, awaitingPong: false };
  socket.setNoDelay(true);
  socket.setTimeout(0); // connections are long-lived; pings handle liveness

  socket.on('drain', () => {
    if (!client.alive || client.queue.length === 0) return;
    // Flush queued frames.
    while (client.queue.length > 0) {
      const frame = client.queue.shift();
      if (!socket.write(frame)) { client.paused = true; return; }
    }
    client.paused = false;
  });

  return client;
}

// Send a text payload to a single client. Handles backpressure by queuing.
function send(client, payload) {
  if (!client.alive) return;
  const frame = encodeFrame(payload);
  if (client.paused || client.queue.length > 0) {
    // Already in backpressure — queue behind existing frames.
    client.queue.push(frame);
    // Cap queue to prevent unbounded growth (drop old frames).
    if (client.queue.length > 20) client.queue.splice(0, client.queue.length - 20);
    return;
  }
  try {
    if (!client.socket.write(frame)) client.paused = true;
  } catch {
    // Socket died mid-write — mark dead so future sends bail; the socket
    // 'close'/'error' handler in handleUpgrade removes it from wsClients.
    client.alive = false;
  }
}

// Send a ping. Doesn't queue — pings are skipped during backpressure.
function ping(client) {
  if (!client.alive || client.paused) return;
  try { client.socket.write(encodePing()); } catch {}
}

// Close a client connection.
function close(client) {
  if (!client.alive) return;
  client.alive = false;
  client.queue.length = 0;
  client.paused = false;
  try {
    // Send close frame (opcode 0x08, status 1000 normal closure).
    const body = Buffer.allocUnsafe(2);
    body.writeUInt16BE(1000, 0);
    client.socket.write(encodeFrame(body, 0x08));
  } catch {}
  try { client.socket.end(); } catch {}
}

// ---- Keepalive (C1) ----
// Half-open WS connections (dead client, live socket) would otherwise leak
// forever: socket.setTimeout(0) disables the TCP timeout, and nothing pings.
// A periodic sweep pings every alive client; any that didn't answer the
// previous ping is half-open and gets closed. The timer lives in state so it
// survives hot reload without accumulating (guarded in start()).
const KEEPALIVE_INTERVAL_MS = 30000; // 30s ping sweep

function sweepClients() {
  for (const client of state.wsClients) {
    if (!client.alive) { state.wsClients.delete(client); continue; }
    if (client.awaitingPong) {
      // No pong since the previous ping — half-open / dead. Close + drop.
      close(client);
      state.wsClients.delete(client);
      continue;
    }
    client.awaitingPong = true;
    ping(client);
  }
}

function startWsKeepalive() {
  if (state.wsKeepaliveTimer) return;
  const t = setInterval(sweepClients, KEEPALIVE_INTERVAL_MS);
  t.unref();
  state.wsKeepaliveTimer = t;
}

module.exports = {
  handshake,
  encodeFrame,
  encodePong,
  parseFrame,
  MAX_FRAME_SIZE,
  addClient,
  send,
  ping,
  pong,
  close,
  startWsKeepalive,
  sweepClients,
};
