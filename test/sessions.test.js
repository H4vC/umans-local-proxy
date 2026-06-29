'use strict';

// Behavior tests for per-session tok/s estimation. The dashboard's live tok/s
// is driven by each in-flight session's own rolling TPS_WINDOW_MS rate (fed by
// the tap worker's ~100ms syncs into session.tpsSamples). The aggregate is the
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

// Build a raw session record. For active sessions, `tps` populates a rolling
// sample history (as the tap worker does) at a steady rate over a 2s span —
// safely inside the 5s window so pruning can't flake on test timing. `age`
// (default 10s) sets the session start and the cumulative outputTokens.
function mk(o = {}) {
  const age = o.age != null ? o.age : 10000;
  const startedAt = o.startedAt != null ? o.startedAt : (Date.now() - age);
  const out = o.outputTokens || 0;
  const s = {
    id: 's' + Math.random().toString(36).slice(2),
    groupKey: 'g1', model: o.model, stream: true, status: o.status,
    startedAt, endedAt: o.endedAt || null,
    outputTokens: out, exactTokens: o.exactTokens || false, chars: o.chars || 0,
    firstTokenAt: o.firstTokenAt || null,
    promptTokens: 100, cachedTokens: 50, completionTokens: out,
    bytes: 0, responseContent: '', finalTps: o.finalTps != null ? o.finalTps : null,
  };
  if (o.status === 'active' && o.tps != null) {
    const t = Date.now();
    const span = 2000;
    const tok0 = (o.tps * Math.max(0, age - span)) / 1000; // cumulative `span` ms ago
    const tok1 = (o.tps * age) / 1000;                      // cumulative now
    s.tpsSamples = [{ ts: t - span, tok: tok0 }, { ts: t, tok: tok1 }];
    s.outputTokens = tok1; // cumulative total
  }
  if (o.model) state.seenModels.add(o.model);
  state.sessions.set(s.id, s);
  return s;
}

test('active session shows its own rolling rate, not a model-median projection', () => {
  reset();
  mk({ model: 'gpt-5', status: 'done', finalTps: 999 }); // historical median 999
  mk({ model: 'gpt-5', status: 'active', tps: 50 });
  const snap = sessions.getSessionsSnapshot();
  const active = snap.sessions.find((s) => s.active);
  assert.equal(active.tps, 50); // its own live rate, not the 999 median
  assert.equal(snap.aggregate.tps, 50);
});

test('per-model live card = median of running sessions rolling rates', () => {
  reset();
  // three running sessions at 40, 50, 100 tok/s
  mk({ model: 'gpt-5', status: 'active', tps: 40 });
  mk({ model: 'gpt-5', status: 'active', tps: 50 });
  mk({ model: 'gpt-5', status: 'active', tps: 100 });
  const snap = sessions.getSessionsSnapshot();
  const m = snap.aggregate.models.find((x) => x.model === 'gpt-5');
  assert.equal(m.tps, 50); // median of [40,50,100] = 50
});

test('aggregate sums each running session live rate (total throughput)', () => {
  reset();
  mk({ model: 'gpt-5', status: 'active', tps: 50 });
  mk({ model: 'claude', status: 'active', tps: 100 });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.aggregate.tps, 150);
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
  mk({ model: 'gpt-5', status: 'active', tps: 50 }); // live 50
  const snap = sessions.getSessionsSnapshot();
  const m = snap.aggregate.models.find((x) => x.model === 'gpt-5');
  assert.equal(m.medianTps, 80); // p50 of [80,100] (nearest-rank lower)
  assert.equal(m.tps, 50); // live card is the running session's own rate
});

test('rolling window does not inflate a long steady stream (the old bug)', () => {
  // Before the fix, a 60s-old steady stream showed a rate that grew without
  // bound (cumulativeTokens / 5000). With the rolling window it shows the true
  // current rate regardless of session age.
  reset();
  mk({ model: 'gpt-5', status: 'active', tps: 10, age: 60000 });
  const snap = sessions.getSessionsSnapshot();
  const active = snap.sessions.find((s) => s.active);
  assert.equal(active.tps, 10); // not 120 (the old 60s inflation)
});

test('rolling window reflects a recent slowdown, not the session average', () => {
  // 100 tok/s for the first 3s, then idle for the last 7s. The rolling 5s
  // window contains no new tokens → ~0, not the 30 tok/s session average.
  reset();
  const t = Date.now();
  const s = {
    id: 's1', groupKey: 'g1', model: 'gpt-5', stream: true, status: 'active',
    startedAt: t - 10000, endedAt: null, outputTokens: 300,
    exactTokens: true, chars: 0, firstTokenAt: null,
    promptTokens: 100, cachedTokens: 50, completionTokens: 300,
    bytes: 0, responseContent: '', finalTps: null,
    tpsSamples: [{ ts: t - 3000, tok: 300 }, { ts: t, tok: 300 }],
  };
  state.seenModels.add('gpt-5');
  state.sessions.set(s.id, s);
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.sessions[0].tps, 0); // no tokens in the rolling window
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
