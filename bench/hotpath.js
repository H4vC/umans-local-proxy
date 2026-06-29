'use strict';
// Per-core hot-path check, post worker-offload. The tap parse runs on a worker
// thread; this measures the CLIENT-VISIBLE rate (pipeline to sink, excluding
// the onEnd telemetry round-trip) vs. the pure-pipe ceiling (no tap), across
// the two chunk regimes. Run: node bench/hotpath.js
//
// Reference (i9-13900K, Node 23):
//   big buffer  (91KB/500 lines): pipe+tap client rate ~4 Gbps (tap parse offloaded)
//   small chunks (693B/3 lines):  pipe+tap client rate ~1.4 Gbps (per-chunk
//     batch-copy + post on the main thread replaces the parse — to go higher,
//     tee the stream so the tap leg transfers chunks zero-copy, no per-chunk
//     main-thread copy).

const { Readable, Writable } = require('node:stream');
const { pipeline: pipelineP } = require('node:stream/promises');
const { ChatTap, createTapStream } = require('../lib/chat-tap');
const state = require('../lib/state');

function buildSse(nChunks, contentLen) {
  const content = 'a'.repeat(contentLen);
  const lines = [];
  for (let i = 0; i < nChunks; i++) {
    lines.push('data: ' + JSON.stringify({
      id: 'c' + i, object: 'chat.completion.chunk', created: 1, model: 'm',
      choices: [{ index: 0, delta: { content }, finish_reason: null }],
    }));
  }
  lines.push('data: ' + JSON.stringify({ id: 'r', choices: [{ delta: { role: 'assistant' } }] }));
  lines.push('data: ' + JSON.stringify({ id: 'f', choices: [{ delta: {}, finish_reason: 'stop' }] }));
  lines.push('data: [DONE]');
  return Buffer.from(lines.join('\n\n') + '\n\n', 'utf8');
}
function fmt(mb) { return mb.toFixed(1).padStart(8) + ' MB/s  ' + (mb / 128).toFixed(2).padStart(5) + ' Gbps'; }
function webStream(chunks) { return new ReadableStream({ start(c) { for (const ch of chunks) c.enqueue(ch); c.close(); } }); }
function sink() { return new Writable({ write(ch, enc, cb) { cb(); } }); }
function makeTap() {
  const s = { id: 'bench-' + Math.random().toString(36).slice(2), bytes:0, chars:0, outputTokens:0, exactTokens:false, responseContent:'', model:'m', firstTokenAt:null, promptTokens:0, cachedTokens:0, completionTokens:0, groupKey:null };
  return { tap: new ChatTap(s, { stream:true, shape:'openai' }), session: s };
}

// pipelineOnly=true: time just the pipe (bytes to sink), excluding onEnd — the
// CLIENT-VISIBLE rate (the worker parse runs in parallel; onEnd only finalizes
// telemetry after the client has all bytes).
async function macro(label, chunks, withTap, pipelineOnly, iters) {
  const totalBytes = chunks.reduce((a, c) => a + c.length, 0);
  const run = async () => {
    if (withTap) { const { tap } = makeTap(); await pipelineP(Readable.fromWeb(webStream(chunks)), createTapStream(tap), sink()); if (!pipelineOnly) await tap.onEnd(); }
    else { await pipelineP(Readable.fromWeb(webStream(chunks)), sink()); }
  };
  for (let i = 0; i < 3; i++) await run();
  const start = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) await run();
  const ns = Number(process.hrtime.bigint() - start);
  console.log(label.padEnd(52), fmt(totalBytes * iters / (ns / 1e9) / (1024 * 1024)));
}

async function main() {
  const big = buildSse(500, 40);
  const small = buildSse(3, 40);
  console.log('big ' + (big.length/1024).toFixed(0) + 'KB/500 lines; small ' + small.length + 'B/3 lines\n');
  await macro('big, pipe + tap (CLIENT rate, no onEnd)', [big], true, true, 200);
  await macro('big, pipe NO tap (ceiling)', [big], false, false, 200);
  await macro('small chunks, pipe + tap (CLIENT rate)', Array(200).fill(small), true, true, 20);
  await macro('small chunks, pipe NO tap (ceiling)', Array(200).fill(small), false, false, 20);
  // terminate the worker so the process exits
  try { if (state.tapWorker) await state.tapWorker.terminate(); } catch {}
}
main().catch((e) => { console.error(e); process.exit(1); });
