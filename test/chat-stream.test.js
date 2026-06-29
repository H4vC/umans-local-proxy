'use strict';

// Hot-path tests for the streaming pipe + SSE pre-filter.
//
// Two layers:
//   1. createTapStream (Transform) + ChatTap pre-filter — isolated: feed SSE
//      through the pipe, assert bytes pass through verbatim AND token counts
//      are correct, including lines the pre-filter must skip (role/finish only).
//   2. proxyRequest end-to-end — a real http.Server running the NEW pipeline
//      code against a mock upstream: bytes forwarded, tokens counted, client
//      disconnect aborts the upstream, and upstream non-2xx status is preserved
//      even for streaming responses.

const { test, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { Readable, Writable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const state = require('../lib/state');
const { ChatTap, createTapStream } = require('../lib/chat-tap');
const { proxyRequest } = require('../lib/chat');
const fs = require('node:fs');
const { BURST_COOLDOWN_FILE } = require('../lib/concurrency');

// The tap worker thread keeps the process alive (unref isn't sufficient in
// Node 23); terminate it after the suite so `node --test` can exit.
after(async () => {
  // Prevent a pending restart timer from re-creating a worker after teardown.
  state.tapWorkerDead = true;
  if (state.tapRestartTimer) { clearTimeout(state.tapRestartTimer); state.tapRestartTimer = null; }
  try { if (state.tapWorker) await state.tapWorker.terminate(); } catch {}
  state.tapWorker = null;
  // The 429 test arms the burst cooldown via the real proxyRequest path, which
  // persists a future epoch to .runtime/burst-cooldown.json. Without this
  // cleanup the next real proxy boot re-arms backoff having never seen a 429.
  state.burstDisabledUntil = 0;
  try { fs.rmSync(BURST_COOLDOWN_FILE, { force: true }); } catch {}
});

// ---- helpers --------------------------------------------------------------

function sseOpenAI() {
  // role-only line (prefilter must skip), two content deltas, usage chunk, [DONE].
  return Buffer.from(
    'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n' +
    'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}\n\n' +
    'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]}\n\n' +
    'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[],"finish_reason":"stop"}\n\n' +
    'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"m","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\n' +
    'data: [DONE]\n\n',
    'utf8',
  );
}

function makeSession() {
  return {
    bytes: 0, chars: 0, outputTokens: 0, exactTokens: false, responseContent: '',
    model: 'm', firstTokenAt: null, promptTokens: 0, cachedTokens: 0, completionTokens: 0,
    groupKey: null,
  };
}

function collect() {
  const chunks = [];
  const sink = new Writable({
    write(chunk, enc, cb) { chunks.push(chunk); cb(); },
  });
  const done = () => Buffer.concat(chunks.map((c) => Buffer.isBuffer(c) ? c : Buffer.from(c)));
  return { sink, done };
}

// ---- 1. createTapStream + pre-filter (isolated) ---------------------------

test('TapStream forwards bytes verbatim and counts tokens (prefilter skips role/finish lines)', async () => {
  state.modelCharRatio.clear();
  const buf = sseOpenAI();
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();

  await pipeline(Readable.from([buf]), createTapStream(tap), sink);
  await tap.onEnd(); // flush pending batches + worker round-trip finalize (mirrors proxyRequest finally)

  // Bytes pass through unchanged.
  assert.deepStrictEqual(done(), buf);
  // Content accumulated from the two content deltas (role/finish skipped).
  assert.strictEqual(session.responseContent, 'Hello world');
  assert.strictEqual(session.chars, 11); // "Hello" + " world"
  // Usage chunk set exact tokens.
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 2);
  assert.strictEqual(session.completionTokens, 2);
  assert.strictEqual(session.promptTokens, 3);
  // bytes counted (every chunk, including skipped-parse ones).
  assert.strictEqual(session.bytes, buf.length);
});

test('TapStream handles chunk boundaries that split an SSE line', async () => {
  state.modelCharRatio.clear();
  const buf = sseOpenAI();
  // Split into 7-byte fragments so SSE lines span chunk boundaries — _drain's
  // lineBuf must reassemble them. This is the realistic undici read regime.
  const fragments = [];
  for (let i = 0; i < buf.length; i += 7) fragments.push(buf.subarray(i, i + 7));
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();

  await pipeline(Readable.from(fragments), createTapStream(tap), sink);
  await tap.onEnd();

  assert.deepStrictEqual(done(), buf);
  assert.strictEqual(session.responseContent, 'Hello world');
  assert.strictEqual(session.chars, 11);
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 2);
});

test('ChatTap repairs tap worker state missing from a hot-reloaded singleton', async () => {
  const prevPending = state.tapPendingFinals;
  const prevWorker = state.tapWorker;
  const prevDead = state.tapWorkerDead;
  const prevRestart = state.tapRestartTimer;
  try {
    delete state.tapPendingFinals;
    delete state.tapWorker;
    delete state.tapWorkerDead;
    delete state.tapRestartTimer;

    const session = makeSession();
    const tap = new ChatTap(session, { stream: true, shape: 'openai' });
    tap.onChunk(Buffer.from('data: [DONE]\n\n'));
    await tap.onEnd();

    assert.ok(state.tapPendingFinals instanceof Map);
    assert.ok(Object.prototype.hasOwnProperty.call(state, 'tapWorker'));
    assert.strictEqual(typeof state.tapWorkerDead, 'boolean');
    assert.ok(Object.prototype.hasOwnProperty.call(state, 'tapRestartTimer'));
  } finally {
    if (state.tapRestartTimer && state.tapRestartTimer !== prevRestart) clearTimeout(state.tapRestartTimer);
    if (state.tapWorker && state.tapWorker !== prevWorker) {
      try { await state.tapWorker.terminate(); } catch {}
    }
    state.tapPendingFinals = prevPending instanceof Map ? prevPending : new Map();
    state.tapWorker = prevWorker || null;
    state.tapWorkerDead = typeof prevDead === 'boolean' ? prevDead : false;
    state.tapRestartTimer = prevRestart || null;
  }
});

// ---- 2. proxyRequest end-to-end -----------------------------------------

function seedState(upstreamBase) {
  state.config = {
    apiKey: 'test-key',
    upstreamBaseURL: upstreamBase,
    enabledModels: [],
    requestTimeout: 30000,
    proxyApiKeys: [],
    overrideConcurrency: 0,
  };
  // Fresh usage/concurrency caches so the throttle admits immediately and
  // refreshUsageSoon() is a no-op (no network fetch of /usage).
  state.usageCache = { data: { ok: true }, time: Date.now() };
  state.concurrencyCache = { concurrent: 0, limit: 100, softLimit: 10, time: Date.now() };
  state.activeRequests = 0;
  state.queuedRequests = 0;
  state.throttleWaiters = [];
  state.burstDisabledUntil = 0;
  state.sessions.clear();
  state.seenModels.clear();
  state.modelCharRatio.clear();
  state.groupSummaries.clear();
  if (state.groupSummaryTimers) state.groupSummaryTimers.clear();
  if (state.stateMapTimers) state.stateMapTimers.clear();
  if (state.sessionsBroadcastTimer) { clearTimeout(state.sessionsBroadcastTimer); state.sessionsBroadcastTimer = null; }
  if (state.refreshUsageTimer) { clearTimeout(state.refreshUsageTimer); state.refreshUsageTimer = null; }
  state.broadcastThrottled = false;
}

function startProxy() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      proxyRequest(req, res, { shape: 'openai' });
    });
    server.listen(0, '127.0.0.1', () => resolve(server));
  });
}

function startUpstream(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => resolve(server));
  });
}

// Await full server close (drop keep-alive connections) so the process can exit.
function closeServer(s) {
  return new Promise((resolve) => {
    if (typeof s.closeAllConnections === 'function') s.closeAllConnections();
    s.close(() => resolve());
  });
}

function bodyOf(req) {
  return JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }], stream: true, ...req });
}

test('streaming: forwards bytes verbatim and counts tokens via the pipe', async () => {
  const sse = sseOpenAI();
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.end(sse);
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: bodyOf({ stream: true }),
  });
  const text = await resp.text();
  await new Promise((r) => setTimeout(r, 5)); // let trailing setImmediate drain flush

  assert.strictEqual(resp.status, 200);
  assert.strictEqual(text, sse.toString('utf8')); // bytes forwarded verbatim

  const session = state.sessions.values().next().value;
  assert.ok(session, 'session recorded');
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 2);
  // onFinalize clears responseContent after coalescing — assert the durable
  // post-finalize outputs instead: status, exact tokens, and a finalTps
  // computed from the counted output tokens.
  assert.strictEqual(session.status, 'done');
  assert.ok(session.finalTps > 0, 'finalTps computed from counted tokens');

  await closeServer(proxy); await closeServer(upstream);
});

test('streaming: injects stream_options.include_usage into the upstream body without corrupting it', async () => {
  // The proxy splices ,"stream_options":{"include_usage":true} before the
  // top-level '}' instead of re-serializing the whole payload. Assert the body
  // the upstream receives is valid JSON, include_usage is set, and a '}'
  // inside a string value survives intact — the insert must target the
  // top-level closer, not a brace in the content.
  const sse = sseOpenAI();
  let captured = null;
  const upstream = await startUpstream((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => { captured = Buffer.concat(chunks).toString('utf8'); res.writeHead(200, { 'Content-Type': 'text/event-stream' }); res.end(sse); });
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: '}{' }], stream: true }),
  });
  await resp.text();
  await new Promise((r) => setTimeout(r, 5));

  assert.ok(captured, 'upstream received a body');
  const parsed = JSON.parse(captured); // throws if the surgical insert corrupted JSON
  assert.strictEqual(parsed.model, 'm');
  assert.deepStrictEqual(parsed.messages, [{ role: 'user', content: '}{' }]);
  assert.deepStrictEqual(parsed.stream_options, { include_usage: true });

  await closeServer(proxy); await closeServer(upstream);
});

test('non-streaming: forwards body and counts exact tokens', async () => {
  const json = Buffer.from(JSON.stringify({
    id: 'x', object: 'chat.completion', model: 'm',
    choices: [{ index: 0, message: { role: 'assistant', content: 'Hi there' }, finish_reason: 'stop' }],
    usage: { prompt_tokens: 4, completion_tokens: 3 },
  }));
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(json);
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }] }),
  });
  const text = await resp.text();
  await new Promise((r) => setTimeout(r, 5));

  assert.strictEqual(resp.status, 200);
  assert.strictEqual(text, json.toString('utf8'));
  const session = state.sessions.values().next().value;
  assert.ok(session);
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 3);
  assert.strictEqual(session.status, 'done');
  assert.ok(session.finalTps > 0, 'finalTps computed from counted tokens');

  await closeServer(proxy); await closeServer(upstream);
});

test('client disconnect mid-stream aborts the upstream connection', async () => {
  // Upstream streams slowly; records whether its response closed BEFORE all
  // chunks were sent (i.e. the proxy aborted the fetch on client disconnect).
  const total = 8;
  const upstreamState = { sent: 0, aborted: false, done: false };
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    let i = 0;
    res.on('close', () => { if (!upstreamState.done) upstreamState.aborted = true; });
    const pump = () => {
      if (i >= total) { upstreamState.done = true; res.end(); return; }
      upstreamState.sent = ++i;
      res.write(`data: {"choices":[{"delta":{"content":"x"}}]}\n\n`);
      setTimeout(pump, 25);
    };
    pump();
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const ac = new AbortController();
  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: bodyOf({ stream: true }),
    signal: ac.signal,
  });
  const reader = resp.body.getReader();
  await reader.read(); // receive first chunk, then bail
  ac.abort();
  try { await reader.read(); } catch {}

  // Give the abort time to propagate: client close -> proxy res close ->
  // controller.abort -> upstream fetch abort -> upstream res close.
  await new Promise((r) => setTimeout(r, 250));

  assert.ok(upstreamState.aborted, `upstream was not aborted (sent ${upstreamState.sent}/${total})`);
  const session = state.sessions.values().next().value;
  assert.ok(session);
  assert.strictEqual(session.status, 'aborted');

  await closeServer(proxy); await closeServer(upstream);
});

test('upstream non-2xx on a streaming request preserves HTTP status', async () => {
  const upstream = await startUpstream((req, res) => {
    res.writeHead(429, { 'Content-Type': 'application/json' });
    res.end(Buffer.from(JSON.stringify({ error: 'rate limited' })));
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: bodyOf({ stream: true }),
  });
  const text = await resp.text();

  assert.strictEqual(resp.status, 429);
  assert.match(text, /rate limited/);
  assert.ok(state.burstDisabledUntil > Date.now());

  await closeServer(proxy); await closeServer(upstream);
});

// ---- scan correctness: escapes, surrogate emoji, deferred content decode ----

test('scan counts chars with escapes + emoji and decodes responseContent (deferred) to match JSON', async () => {
  state.modelCharRatio.clear();
  // newline, escaped quotes, escaped backslash, a surrogate-pair emoji, non-ASCII
  const content = 'line1\nline2 "quoted" \\backslash \uD83D\uDE00 café end';
  const reasoning = 'thinking about it';
  const lines = [
    'data: ' + JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', model: 'm', choices: [{ index: 0, delta: { content }, finish_reason: null }] }),
    'data: ' + JSON.stringify({ id: 'c2', object: 'chat.completion.chunk', model: 'm', choices: [{ index: 0, delta: { reasoning_content: reasoning }, finish_reason: null }] }),
    'data: ' + JSON.stringify({ id: 'u', object: 'chat.completion.chunk', model: 'm', choices: [], usage: { prompt_tokens: 5, completion_tokens: 7 } }),
    'data: [DONE]',
  ];
  const buf = Buffer.from(lines.join('\n\n') + '\n\n', 'utf8');
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();

  await pipeline(Readable.from([buf]), createTapStream(tap), sink);
  await tap.onEnd();

  assert.deepStrictEqual(done(), buf); // bytes forwarded verbatim
  // Both content and reasoning contribute to the char estimate.
  assert.strictEqual(session.chars, content.length + reasoning.length);
  // responseContent is the DECODED content only (reasoning excluded), via the
  // deferred decode at _finish — must match what JSON.parse would produce.
  assert.strictEqual(session.responseContent, content);
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 7);
});

test('scan does not false-match a "content":" literal embedded in the content value', async () => {
  state.modelCharRatio.clear();
  // The value itself contains the bytes "content":" — JSON escapes the quotes,
  // so the scan must find only the real key and stop at the real closing quote.
  const content = 'the key "content":"x" is here';
  const buf = Buffer.from(
    'data: ' + JSON.stringify({ id: 'c', object: 'chat.completion.chunk', model: 'm', choices: [{ index: 0, delta: { content }, finish_reason: null }] }) + '\n\n' +
    'data: [DONE]\n\n', 'utf8');
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();

  await pipeline(Readable.from([buf]), createTapStream(tap), sink);
  await tap.onEnd();

  assert.deepStrictEqual(done(), buf);
  assert.strictEqual(session.chars, content.length);
  assert.strictEqual(session.responseContent, content);
});

test('Anthropic streaming: counts text+thinking, decodes responseContent, exact usage', async () => {
  state.modelCharRatio.clear();
  // Anthropic emits event:/data: pairs; only data: lines parse. text_delta
  // accumulates responseContent; thinking_delta counts chars only.
  const buf = Buffer.from(
    'event: message_start\n' +
    'data: {"type":"message_start","message":{"usage":{"input_tokens":5,"cache_read_input_tokens":2}}}\n\n' +
    'event: content_block_delta\n' +
    'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello world"}}\n\n' +
    'event: content_block_delta\n' +
    'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning"}}\n\n' +
    'event: message_delta\n' +
    'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}\n\n' +
    'event: message_stop\n' +
    'data: [DONE]\n\n', 'utf8');
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'anthropic' });
  const { sink, done } = collect();

  await pipeline(Readable.from([buf]), createTapStream(tap), sink);
  await tap.onEnd();

  assert.deepStrictEqual(done(), buf);
  assert.strictEqual(session.chars, 'Hello world'.length + 'reasoning'.length);
  assert.strictEqual(session.responseContent, 'Hello world'); // thinking excluded
  assert.strictEqual(session.exactTokens, true);
  assert.strictEqual(session.outputTokens, 7);
  assert.strictEqual(session.promptTokens, 5);
  assert.strictEqual(session.cachedTokens, 2);
});

test('tap decodes multibyte UTF-8 split across fragments (no U+FFFD)', async () => {
  state.modelCharRatio.clear();
  // "héllo" — é is 0xC3 0xA9. Fragment the SSE so the two bytes land in
  // separate fragments; a per-buffer decode would emit U+FFFD for each half.
  const delta = 'data: {"choices":[{"index":0,"delta":{"content":"héllo"},"finish_reason":null}]}\n\n';
  const buf = Buffer.from(delta, 'utf8');
  const split = buf.indexOf(Buffer.from([0xC3])) + 1; // between 0xC3 and 0xA9
  const fragments = [buf.subarray(0, split), buf.subarray(split)];
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();
  await pipeline(Readable.from(fragments), createTapStream(tap), sink);
  await tap.onEnd();
  assert.deepStrictEqual(done(), buf);
  assert.strictEqual(session.responseContent, 'héllo');
  assert.strictEqual(session.chars, 5);
});

test('tap carries split multibyte UTF-8 across batch flushes (streaming decoder)', async () => {
  state.modelCharRatio.clear();
  // Force the two bytes of é into separate worker batches via _flush; only a
  // streaming TextDecoder (stream:true, carrying state across feed() calls)
  // survives this — concat-before-decode alone would not.
  const delta = 'data: {"choices":[{"index":0,"delta":{"content":"héllo"},"finish_reason":null}]}\n\n';
  const buf = Buffer.from(delta, 'utf8');
  const split = buf.indexOf(Buffer.from([0xC3])) + 1;
  const a = buf.subarray(0, split);
  const b = buf.subarray(split);
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  tap.onChunk(a); tap._flush();
  tap.onChunk(b);
  await tap.onEnd();
  assert.strictEqual(session.responseContent, 'héllo');
  assert.strictEqual(session.chars, 5);
});

test('OpenAI content delta whose JSON contains "usage" is still scanned', async () => {
  state.modelCharRatio.clear();
  // A content delta whose JSON literally contains "usage" (here as a value)
  // must not be short-circuited by the usage fast-path — its content is scanned.
  const sse = Buffer.from(
    'data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}],"meta":"usage"}\n\n' +
    'data: [DONE]\n\n',
    'utf8',
  );
  const session = makeSession();
  const tap = new ChatTap(session, { stream: true, shape: 'openai' });
  const { sink, done } = collect();
  await pipeline(Readable.from([sse]), createTapStream(tap), sink);
  await tap.onEnd();
  assert.strictEqual(session.responseContent, 'hi'); // not dropped by the usage fast-path
  assert.ok(session.chars >= 2);
});

// ---- C6: mid-stream upstream error → terminal SSE error frame ----

test('mid-stream upstream error emits a terminal SSE error frame (C6)', async () => {
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.write('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n');
    // Kill the socket mid-stream AFTER the proxy has received headers and
    // started piping — undici surfaces this as a body-stream error, which the
    // proxy must convert to a terminal SSE error frame instead of a silent
    // truncation (clients can't otherwise tell a real failure from a clean end).
    setTimeout(() => { try { res.socket.destroy(new Error('upstream stream died')); } catch {} }, 100);
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);

  const resp = await fetch(`http://127.0.0.1:${proxy.address().port}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: bodyOf({ stream: true }),
  });
  assert.strictEqual(resp.status, 200); // headers already sent before the error
  const text = await resp.text();
  await new Promise((r) => setTimeout(r, 5)); // let finalize drain

  assert.match(text, /partial/);                        // partial content delivered
  assert.match(text, /data: \{"error":\{"message":/);  // C6 terminal error frame
  assert.match(text, /data: \[DONE\]/);                 // terminal marker
  const session = state.sessions.values().next().value;
  assert.ok(session);
  assert.strictEqual(session.status, 'error');          // real failure, not 'done'

  await closeServer(proxy); await closeServer(upstream);
});

// ---- C8: ChatTap tracks active taps so a worker restart can re-init them ----

test('ChatTap tracks active taps for worker-restart re-init (C8)', async () => {
  const prevActive = state.tapActiveTaps;
  const prevPending = state.tapPendingFinals;
  const prevWorker = state.tapWorker;
  const prevDead = state.tapWorkerDead;
  const prevRestart = state.tapRestartTimer;
  try {
    state.tapActiveTaps = new Map();
    const session = makeSession();
    const tap = new ChatTap(session, { stream: true, shape: 'openai' });
    // Constructed with a model → registered so a restart can re-init its parser.
    assert.ok(state.tapActiveTaps.has(tap.id), 'tap registered on construct');
    tap.onChunk(Buffer.from('data: [DONE]\n\n'));
    await tap.onEnd();
    // Finalized → unregistered (a restart must not re-init a completed tap).
    assert.ok(!state.tapActiveTaps.has(tap.id), 'tap unregistered on finalize');
  } finally {
    if (state.tapRestartTimer && state.tapRestartTimer !== prevRestart) clearTimeout(state.tapRestartTimer);
    if (state.tapWorker && state.tapWorker !== prevWorker) { try { await state.tapWorker.terminate(); } catch {} }
    state.tapActiveTaps = prevActive instanceof Map ? prevActive : new Map();
    state.tapPendingFinals = prevPending instanceof Map ? prevPending : new Map();
    state.tapWorker = prevWorker || null;
    state.tapWorkerDead = typeof prevDead === 'boolean' ? prevDead : false;
    state.tapRestartTimer = prevRestart || null;
  }
});

// ---- coalescing: 2-turn grouping through the full proxyRequest path ----

test('coalescing: turn 2 joins turn 1 group when the assistant replay matches', async () => {
  const sse = (content) => Buffer.from(
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n' +
    `data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":${JSON.stringify(content)}}]}\n\n` +
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[],"finish_reason":"stop"}\n\n' +
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}\n\n' +
    'data: [DONE]\n\n', 'utf8');
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.end(sse('Hello'));
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);
  for (const t of state.stateMapTimers.values()) clearTimeout(t);
  state.stateMap.clear(); state.stateMapTimers.clear();
  state.messageHashCache.clear();

  const port = proxy.address().port;
  // Turn 1: [user "hi"] -> assistant "Hello" (proxy captures responseContent "Hello").
  const r1 = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }], stream: true }) });
  await r1.text();
  await new Promise((r) => setTimeout(r, 15)); // let tap.onEnd + storeStateKey finalize
  const s1 = [...state.sessions.values()].find((s) => s.status === 'done');
  assert.ok(s1, 'turn1 session recorded');
  const group1 = s1.groupKey;

  // Turn 2: client replays the assistant verbatim + a new user message.
  const r2 = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }, { role: 'assistant', content: 'Hello' }, { role: 'user', content: 'bye' }], stream: true }) });
  await r2.text();
  await new Promise((r) => setTimeout(r, 15));
  const s2 = [...state.sessions.values()].filter((s) => s !== s1).pop();
  assert.ok(s2 && s2 !== s1, 'turn2 session recorded');
  assert.strictEqual(s2.groupKey, group1, 'turn2 must coalesce into turn1 group (replay matched responseContent)');

  await closeServer(proxy); await closeServer(upstream);
});

// ---- live tok/s + TTFT surface during streaming (E2E) ----

test('live snapshot shows tps > 0 and ttftMs during an active stream', async () => {
  // Stream SSE chunks with 50ms gaps over ~700ms so the session stays active
  // long enough for the tap worker's 100ms sync throttle to fire multiple
  // times with non-zero outputTokens. The first sync (from the role-only
  // chunk) carries outputTokens=0; real content syncs land ~100ms later.
  const chunks = [
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"Hello "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"world "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"streaming "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"tokens "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"through "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"the "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"pipe "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"now "}}]}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[],"finish_reason":"stop"}\n\n',
    'data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":9}}\n\n',
    'data: [DONE]\n\n',
  ];
  const upstream = await startUpstream((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    let i = 0;
    const send = () => {
      if (i < chunks.length) { res.write(chunks[i++]); setTimeout(send, 50); }
      else res.end();
    };
    setTimeout(send, 50);
  });
  const proxy = await startProxy();
  seedState(`http://127.0.0.1:${upstream.address().port}`);
  const { getSessionsSnapshot } = require('../lib/sessions');

  const port = proxy.address().port;
  const fetchPromise = fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }], stream: true }),
  });

  // Wait until multiple content syncs have fired (~350ms: role sync at ~70ms,
  // first content sync at ~170ms, second at ~270ms — all well within the
  // ~600ms stream).
  await new Promise((r) => setTimeout(r, 350));
  const snap = getSessionsSnapshot();
  const active = snap.sessions.find((s) => s.active);
  assert.ok(active, 'an active session exists mid-stream');
  assert.ok(active.tps > 0, `active session tps > 0 (got ${active.tps})`);
  assert.ok(active.ttftMs != null && active.ttftMs >= 0, `active session ttftMs set (got ${active.ttftMs})`);

  // Let the stream finish and check the finalized session.
  const resp = await fetchPromise;
  await resp.text();
  await new Promise((r) => setTimeout(r, 30)); // let tap.onEnd + finalizeSession run
  const snap2 = getSessionsSnapshot();
  const done = snap2.sessions.find((s) => !s.active);
  assert.ok(done, 'a finished session exists');
  assert.ok(done.finalTps > 0, `finished session finalTps > 0 (got ${done.finalTps})`);
  assert.ok(done.ttftMs != null && done.ttftMs >= 0, `finished session ttftMs set (got ${done.ttftMs})`);
  assert.ok(snap2.aggregate.ttftMsP50 != null, 'aggregate ttftMsP50 set from finished sessions');
  assert.ok(snap2.aggregate.medianTps != null, 'aggregate medianTps (tok/s p50) set from finished sessions');

  await closeServer(proxy); await closeServer(upstream);
});
