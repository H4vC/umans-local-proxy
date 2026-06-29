'use strict';

// Unit tests for the minimal WebSocket frame layer (lib/ws.js): parseFrame
// hardening (masked-only, control-frame ≤125, tooLarge) and pong encoding.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const ws = require('../lib/ws');

// Build a masked client frame (RFC 6455 §5.1 mandates client masking).
function maskedFrame(opcode, payload, mask = Buffer.from([1, 2, 3, 4])) {
  const plen = payload.length;
  let header;
  if (plen < 126) {
    header = Buffer.allocUnsafe(2);
    header[1] = 0x80 | plen;
  } else if (plen < 65536) {
    header = Buffer.allocUnsafe(4);
    header[1] = 0x80 | 126;
    header.writeUInt16BE(plen, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(plen), 2);
  }
  header[0] = 0x80 | opcode; // FIN + opcode
  const masked = Buffer.allocUnsafe(plen);
  for (let i = 0; i < plen; i++) masked[i] = payload[i] ^ mask[i & 3];
  return Buffer.concat([header, mask, masked]);
}

test('parseFrame decodes a masked text frame and unmasks the payload', () => {
  const f = ws.parseFrame(maskedFrame(0x01, Buffer.from('hello')));
  assert.strictEqual(f.type, 0x01);
  assert.strictEqual(f.data.toString(), 'hello');
  assert.ok(f.consumed > 0);
});

test('parseFrame rejects an unmasked client frame as protocol (§5.1)', () => {
  const unmasked = Buffer.from([0x81, 0x05, 0x68, 0x69]); // FIN+text, len 5, no mask bit
  assert.strictEqual(ws.parseFrame(unmasked).type, 'protocol');
});

test('parseFrame rejects an oversized control frame as protocol (§5.5)', () => {
  // ping (0x09) declaring a 126-byte payload via extended length — control
  // frames must carry ≤125 bytes.
  const f = ws.parseFrame(maskedFrame(0x09, Buffer.alloc(126)));
  assert.strictEqual(f.type, 'protocol');
});

test('parseFrame accepts a small ping and returns its unmasked payload', () => {
  const f = ws.parseFrame(maskedFrame(0x09, Buffer.from('ping!')));
  assert.strictEqual(f.type, 0x09);
  assert.strictEqual(f.data.toString(), 'ping!');
});

test('parseFrame returns tooLarge for a data frame over MAX_FRAME_SIZE', () => {
  const huge = Buffer.allocUnsafe(10);
  huge[0] = 0x81;             // FIN + text
  huge[1] = 0x80 | 127;       // masked + 64-bit length
  huge.writeBigUInt64BE(BigInt(ws.MAX_FRAME_SIZE + 1), 2);
  assert.strictEqual(ws.parseFrame(huge).type, 'tooLarge');
});

test('parseFrame returns null for a truncated/incomplete frame', () => {
  assert.strictEqual(ws.parseFrame(Buffer.from([0x81])), null);
  assert.strictEqual(ws.parseFrame(Buffer.alloc(0)), null);
});

test('encodePong builds a FIN+pong frame echoing the payload', () => {
  const frame = ws.encodePong(Buffer.from('abc'));
  assert.strictEqual(frame[0], 0x8A); // FIN + pong
  assert.strictEqual(frame[1], 3);     // payload len (≤125, unmasked: server→client)
  assert.strictEqual(frame.subarray(2).toString(), 'abc');
});

test('encodePong with no payload is a 2-byte frame', () => {
  const frame = ws.encodePong(null);
  assert.strictEqual(frame.length, 2);
  assert.strictEqual(frame[0], 0x8A);
  assert.strictEqual(frame[1], 0);
});

// ---- C1: sweepClients half-open detection + C10: send try/catch ----

test('sweepClients closes a client that missed its pong (half-open, C1)', () => {
  const state = require('../lib/state');
  const origClients = state.wsClients;
  const clients = new Set();
  state.wsClients = clients;
  const client = { socket: { write: () => true, end: () => {} }, queue: [], paused: false, alive: true, awaitingPong: true };
  clients.add(client);
  try {
    ws.sweepClients();
    assert.strictEqual(client.alive, false, 'half-open client (awaitingPong) is closed');
    assert.strictEqual(clients.has(client), false, 'closed client removed from the set');
  } finally {
    state.wsClients = origClients;
  }
});

test('sweepClients pings a live client and marks it awaitingPong (C1)', () => {
  const state = require('../lib/state');
  const origClients = state.wsClients;
  const clients = new Set();
  state.wsClients = clients;
  let writes = 0;
  const client = { socket: { write: () => { writes++; return true; }, end: () => {} }, queue: [], paused: false, alive: true, awaitingPong: false };
  clients.add(client);
  try {
    ws.sweepClients();
    assert.strictEqual(writes, 1, 'live client is pinged (one frame written)');
    assert.strictEqual(client.awaitingPong, true, 'awaitingPong set for the next sweep');
    assert.strictEqual(client.alive, true, 'live client stays alive');
  } finally {
    state.wsClients = origClients;
  }
});

test('send swallows a write error on a dead socket instead of throwing (C10)', () => {
  const client = { socket: { write: () => { throw new Error('EPIPE'); } }, queue: [], paused: false, alive: true };
  assert.doesNotThrow(() => ws.send(client, 'hello'));
  assert.strictEqual(client.alive, false, 'dead socket marks the client not alive');
});
