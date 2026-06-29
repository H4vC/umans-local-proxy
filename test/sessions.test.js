'use strict';

// Behavior tests for per-session tok/s estimation. The dashboard's tok/s is
// driven by each in-flight session's own estimated live rate (chars→tokens via
// the learned per-model ratio, over a rolling window). The aggregate is the
// sum of active sessions' rates; the per-model "live" card value is the median
// of that model's running sessions' rates. Finished sessions show their own
// measured finalTps.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const state = require('../lib/state');
const sessions = require('../lib/sessions');

function reset() {
  state.sessions.clear();
  state.seenModels.clear();
  state.groupSummaries.clear();
  if (state.groupSummaryTimers) state.groupSummaryTimers.clear();
  if (state.modelCharRatio) state.modelCharRatio.clear();
}

// Build a raw session record. startedAt defaults to 10s ago so elapsed >
// TPS_WINDOW_MS, making sessionTps = outputTokens / TPS_WINDOW_MS * 1000
// deterministic (window is fixed at the 5s cap regardless of test timing).
const T0 = Date.now() - 10000;
function mk(o) {
  const out = o.outputTokens || 0;
  const s = {
    id: 's' + Math.random().toString(36).slice(2),
    groupKey: 'g1', model: o.model, stream: true, status: o.status,
    startedAt: o.startedAt || T0, endedAt: o.endedAt || null,
    outputTokens: out, exactTokens: o.exactTokens || false, chars: o.chars || 0,
    firstTokenAt: o.firstTokenAt || null,
    promptTokens: 100, cachedTokens: 50, completionTokens: out,
    bytes: 0, responseContent: '', finalTps: o.finalTps != null ? o.finalTps : null,
  };
  if (o.model) state.seenModels.add(o.model);
  state.sessions.set(s.id, s);
  return s;
}
// expected sessionTps for a given outputTokens count (window = TPS_WINDOW_MS = 5000)
const rate = (tokens) => Math.round((tokens / 5000) * 1000 * 10) / 10;

test('active session shows its own estimated live rate, not a model-median projection', () => {
  reset();
  mk({ model: 'gpt-5', status: 'done', finalTps: 999 }); // historical median 999
  mk({ model: 'gpt-5', status: 'active', outputTokens: 250 });
  const snap = sessions.getSessionsSnapshot();
  const active = snap.sessions.find((s) => s.active);
  assert.equal(active.tps, rate(250)); // 50 tok/s, estimated from its own tokens
  assert.equal(snap.aggregate.tps, rate(250));
});

test('per-model live card = median of running sessions estimated rates', () => {
  reset();
  // three running sessions at 40, 50, 100 tok/s
  mk({ model: 'gpt-5', status: 'active', outputTokens: 200 });
  mk({ model: 'gpt-5', status: 'active', outputTokens: 500 });
  mk({ model: 'gpt-5', status: 'active', outputTokens: 250 });
  const snap = sessions.getSessionsSnapshot();
  const m = snap.aggregate.models.find((x) => x.model === 'gpt-5');
  assert.equal(m.tps, rate(250)); // median of [40,50,100] = 50
});

test('aggregate sums each running session live rate (total throughput)', () => {
  reset();
  mk({ model: 'gpt-5', status: 'active', outputTokens: 250 }); // 50
  mk({ model: 'claude', status: 'active', outputTokens: 500 }); // 100
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.aggregate.tps, Math.round((rate(250) + rate(500)) * 10) / 10);
  assert.equal(snap.aggregate.activeSessions, 2);
});

test('finished session shows its own measured finalTps, not an estimate', () => {
  reset();
  mk({ model: 'gpt-5', status: 'done', finalTps: 80, endedAt: Date.now() });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.sessions[0].tps, 80);
  assert.equal(snap.sessions[0].active, false);
  assert.equal(snap.aggregate.tps, 0); // no active sessions
  assert.equal(snap.aggregate.activeSessions, 0);
});

test('historical medianTps/p10 come from finished-turn finalTps, independent of live', () => {
  reset();
  mk({ model: 'gpt-5', status: 'done', finalTps: 80 });
  mk({ model: 'gpt-5', status: 'done', finalTps: 100 });
  mk({ model: 'gpt-5', status: 'active', outputTokens: 250 }); // live 50
  const snap = sessions.getSessionsSnapshot();
  const m = snap.aggregate.models.find((x) => x.model === 'gpt-5');
  assert.equal(m.medianTps, 80); // p50 of [80,100] (nearest-rank lower)
  assert.equal(m.tps, rate(250)); // live card is the running session's own rate
});

test('estimateTokensFromChars falls back to chars/4 with no learned ratio', () => {
  reset();
  assert.equal(sessions.estimateTokensFromChars('never-seen-model', 40), 10);
});

test('session view exposes exactTokens (exact once usage arrives)', () => {
  reset();
  mk({ model: 'gpt-5', status: 'active', outputTokens: 250, exactTokens: true });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.sessions[0].exactTokens, true);
});
