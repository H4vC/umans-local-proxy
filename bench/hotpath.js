'use strict';
// Per-core hot-path regression check. Measures the end-to-end pipeline ceiling
// with the production ChatTap (scan-based drain) vs. the pure-pipe ceiling (no
// tap), across the two realistic chunk regimes. Run: node bench/hotpath.js
//
// Reference (i9-13900K, Node 23, post scan+pipe+prefilter):
//   big buffer  (91KB/500 lines): ~1.7 Gbps with tap, ~19 Gbps without
//   small chunks (693B/3 lines):  ~1.3 Gbps with tap, ~8  Gbps without
// The tap (deferred scan drain) is the remaining per-core cost; the pure pipe
// is 6-15x the tap-included rate, so the tap is the next ceiling if you need
// more (offload to a worker thread, or coalesce small chunks to raise the
// small-chunk pipe ceiling itself).

const { Readable, Writable } = require('node:stream');
const { pipeline: pipelineP } = require('node:stream/promises');
const { ChatTap, createTapStream } = require('../lib/chat-tap');

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
  const s = { bytes:0, chars:0, outputTokens:0, exactTokens:false, responseContent:'', model:'m', firstTokenAt:null, promptTokens:0, cachedTokens:0, completionTokens:0, groupKey:null };
  return { tap: new ChatTap(s, { stream:true, shape:'openai' }), session: s };
}

async function macro(label, chunks, withTap, iters) {
  const totalBytes = chunks.reduce((a, c) => a + c.length, 0);
  const run = async () => {
    if (withTap) { const { tap } = makeTap(); await pipelineP(Readable.fromWeb(webStream(chunks)), createTapStream(tap), sink()); tap.onEnd(); }
    else { await pipelineP(Readable.fromWeb(webStream(chunks)), sink()); }
  };
  for (let i = 0; i < 3; i++) await run();
  const start = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) await run();
  const ns = Number(process.hrtime.bigint() - start);
  console.log(label.padEnd(46), fmt(totalBytes * iters / (ns / 1e9) / (1024 * 1024)));
}

async function main() {
  const big = buildSse(500, 40);
  const small = buildSse(3, 40);
  console.log('big ' + (big.length/1024).toFixed(0) + 'KB/500 lines; small ' + small.length + 'B/3 lines\n');
  await macro('big, pipe + tap (production)', [big], true, 200);
  await macro('big, pipe NO tap (ceiling)', [big], false, 200);
  await macro('small chunks, pipe + tap (production)', Array(200).fill(small), true, 20);
  await macro('small chunks, pipe NO tap (ceiling)', Array(200).fill(small), false, 20);
}
main().catch((e) => { console.error(e); process.exit(1); });
