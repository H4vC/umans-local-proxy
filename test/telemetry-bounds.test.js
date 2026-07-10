'use strict';
require('./fetch-guard');

const { test, after } = require('node:test');
const assert = require('node:assert/strict');

const state = require('../lib/state');
const { TapParser, MAX_SSE_LINE_CHARS, MAX_RESPONSE_CONTENT_CHARS } = require('../lib/tap-worker');
const { ChatTap, TAP_PENDING_BYTES_MAX, GLOBAL_PENDING_BYTES_MAX, MAX_TPS_SAMPLES, onWorkerMessage } = require('../lib/chat-tap');
const { chainHash, storeStateKey, logCoalesce } = require('../lib/coalesce');

function session() {
  return {
    id: 'tap-' + Math.random().toString(36).slice(2),
    model: 'm', bytes: 0, chars: 0, outputTokens: 0, exactTokens: false,
    responseContent: '', promptTokens: 0, cachedTokens: 0, completionTokens: 0,
    firstTokenAt: null,
  };
}

function waitFor(predicate, timeoutMs = 1000) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const poll = () => {
      if (predicate()) return resolve();
      if (Date.now() >= deadline) return reject(new Error('condition did not become true'));
      setTimeout(poll, 5);
    };
    poll();
  });
}

function clearCoalesceState() {
  for (const timer of state.stateMapTimers.values()) clearTimeout(timer);
  state.stateMap.clear();
  state.stateMapTimers.clear();
  state.coalesceDebug.length = 0;
}

after(async () => {
  clearCoalesceState();
  state.tapWorkerDead = true;
  if (state.tapRestartTimer) {
    clearTimeout(state.tapRestartTimer);
    state.tapRestartTimer = null;
  }
  try { if (state.tapWorker) await state.tapWorker.terminate(); } catch {}
  state.tapWorker = null;
  state.tapPendingBytes = 0;
  state.tapPendingTaps?.clear();
  state.tapActiveTaps?.clear();
});

test('unterminated SSE line is discarded at a fixed bound and later records still parse', () => {
  const parser = new TapParser({ shape: 'openai', model: 'm', stream: true });
  parser.feed(Buffer.from('data: ' + 'x'.repeat(MAX_SSE_LINE_CHARS + 1)));
  assert.equal(parser.lineBuf, '');
  assert.equal(parser.lineDiscarding, true);

  parser.feed(Buffer.from('\ndata: {"choices":[{"delta":{"content":"ok"}}]}\n\n'));
  const fields = parser.finish();
  assert.equal(fields.chars, 2);
  assert.equal(fields.responseContent, 'ok');
});

test('response capture disables coalescing content at its bound but retains exact counters', () => {
  const parser = new TapParser({ shape: 'openai', model: 'm', stream: true });
  const delta = 'x'.repeat(1024);
  const chunks = [];
  const count = Math.floor(MAX_RESPONSE_CONTENT_CHARS / delta.length) + 1;
  for (let i = 0; i < count; i++) {
    chunks.push('data: ' + JSON.stringify({ choices: [{ delta: { content: delta } }] }) + '\n\n');
  }
  chunks.push('data: ' + JSON.stringify({ usage: { prompt_tokens: 3, completion_tokens: 7 } }) + '\n\n');
  parser.feed(Buffer.from(chunks.join('')));
  const fields = parser.finish();
  assert.equal(fields.chars, count * delta.length);
  assert.equal(fields.responseContent, '');
  assert.equal(fields.exactTokens, true);
  assert.equal(fields.outputTokens, 7);
  assert.equal(fields.promptTokens, 3);
});

test('tap saturation preserves forwarded-byte accounting while dropping telemetry', async () => {
  const s = session();
  const tap = new ChatTap(s, { stream: true, shape: 'openai' });
  const oversized = Buffer.alloc(TAP_PENDING_BYTES_MAX + 1, 0x78);
  tap.onChunk(oversized);
  assert.equal(s.bytes, oversized.length);
  assert.equal(tap.pendingBytes, 0);
  assert.equal(tap.telemetryDropped, true);
  await tap.onEnd();
});

test('global tap credit rejects optional telemetry without changing byte accounting', async () => {
  const originalPending = state.tapPendingBytes;
  const s = session();
  const tap = new ChatTap(s, { stream: true, shape: 'openai' });
  try {
    state.tapPendingBytes = GLOBAL_PENDING_BYTES_MAX - 1;
    tap.onChunk(Buffer.from('xx'));
    assert.equal(s.bytes, 2);
    assert.equal(tap.pendingBytes, 0);
    assert.equal(tap.telemetryDropped, true);
  } finally {
    state.tapPendingBytes = originalPending;
    await tap.onEnd();
  }
});

test('worker acknowledgement replenishes a tap credit after a transferred batch', async () => {
  const s = session();
  const tap = new ChatTap(s, { stream: true, shape: 'openai' });
  const chunk = Buffer.from('data: {"choices":[{"delta":{"content":"ack"}}]}\n\n');
  tap.onChunk(chunk);
  tap._flush();
  assert.equal(tap.pendingBytes, chunk.length, 'credit stays consumed until worker ack');
  await waitFor(() => tap.pendingBytes === 0);
  await tap.onEnd();
  assert.equal(s.responseContent, 'ack');
});

test('coalescing state evicts its oldest entry at the configured retention limit', () => {
  clearCoalesceState();
  const original = state.config;
  state.config = { ...(original || {}), limits: { ...(original?.limits || {}), stateMap: 2 } };
  try {
    state.stateMap.set('old-0', 'g-0');
    state.stateMap.set('old-1', 'g-1');
    const messages = [{ role: 'user', content: 'new' }];
    storeStateKey('m', messages, 'answer', 'new-group', chainHash('m', []));
    assert.equal(state.stateMap.size, 2);
    assert.equal(state.stateMap.has('old-0'), false);
    assert.equal([...state.stateMap.values()].includes('new-group'), true);
  } finally {
    state.config = original;
  }
  const originalDebugMax = state.COALESCE_DEBUG_MAX;
  try {
    state.COALESCE_DEBUG_MAX = 2;
    logCoalesce({ n: 1 });
    logCoalesce({ n: 2 });
    logCoalesce({ n: 3 });
    assert.equal(state.coalesceDebug.length, 2);
  } finally {
    state.COALESCE_DEBUG_MAX = originalDebugMax;
  }
});

test('discard drops unflushed parser telemetry without changing recorded bytes', async () => {
  const s = session();
  const tap = new ChatTap(s, { stream: true, shape: 'openai' });
  const chunk = Buffer.from('data: {"choices":[{"delta":{"content":"discard"}}]}\n\n');
  tap.onChunk(chunk);
  assert.equal(tap.pendingBytes, chunk.length);
  tap.discard();
  assert.equal(tap.pendingBytes, 0);
  assert.equal(s.bytes, chunk.length);
  await tap.onEnd();
});

test('live TPS samples stay bounded even when syncs arrive faster than the time window', () => {
  const id = 'sample-' + Math.random().toString(36).slice(2);
  const s = { id, startedAt: Date.now(), outputTokens: 0, tpsSamples: [], tpsHead: 0 };
  state.sessions.set(id, s);
  try {
    for (let i = 0; i <= MAX_TPS_SAMPLES; i++) onWorkerMessage({ op: 'sync', id, fields: { outputTokens: i } });
    assert.ok(s.tpsSamples.length <= MAX_TPS_SAMPLES);
  } finally {
    state.sessions.delete(id);
  }
});
