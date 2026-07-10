'use strict';
require('./fetch-guard');

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
  state.completedSessionOrder.length = 0;
  state.completedSessionHead = 0;
  for (const timer of state.sessionTimers.values()) clearTimeout(timer);
  state.sessionTimers.clear();
  state.sessionsByGroup?.clear();
  state.seenModels.clear();
  state.groupSummaries.clear();
  if (state.groupSummaryTimers) {
    for (const timer of state.groupSummaryTimers.values()) clearTimeout(timer);
    state.groupSummaryTimers.clear();
  }
  if (state.modelCharRatio) state.modelCharRatio.clear();
}

function withLimits(overrides, fn) {
  const original = state.config;
  state.config = { ...(original || {}), limits: { ...(original?.limits || {}), ...overrides } };
  try { return fn(); } finally { state.config = original; }
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

// ---- proxy-derived TTFT metrics (local, not upstream) ----

test('session view exposes ttftMs from firstTokenAt', () => {
  reset();
  const startedAt = Date.now() - 5000;
  mk({ model: 'gpt-5', status: 'done', startedAt, firstTokenAt: startedAt + 800, finalTps: 50, endedAt: Date.now() });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.sessions[0].ttftMs, 800);
});

test('session view ttftMs is null when firstTokenAt is missing', () => {
  reset();
  mk({ model: 'gpt-5', status: 'done', finalTps: 50, endedAt: Date.now() });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.sessions[0].ttftMs, null);
});

test('aggregate ttftMsP50 is the median of finished sessions TTFT', () => {
  reset();
  const t = Date.now() - 10000;
  mk({ model: 'gpt-5', status: 'done', startedAt: t, firstTokenAt: t + 100, finalTps: 50, endedAt: Date.now() });
  mk({ model: 'gpt-5', status: 'done', startedAt: t, firstTokenAt: t + 300, finalTps: 60, endedAt: Date.now() });
  mk({ model: 'gpt-5', status: 'done', startedAt: t, firstTokenAt: t + 500, finalTps: 70, endedAt: Date.now() });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.aggregate.ttftMsP50, 300, 'p50 = median of {100,300,500}');
});

test('aggregate ttftMsP50 excludes active sessions', () => {
  reset();
  const t = Date.now() - 10000;
  mk({ model: 'gpt-5', status: 'done', startedAt: t, firstTokenAt: t + 200, finalTps: 50, endedAt: Date.now() });
  mk({ model: 'gpt-5', status: 'active', startedAt: t, firstTokenAt: t + 9999, tps: 30 });
  const snap = sessions.getSessionsSnapshot();
  assert.equal(snap.aggregate.ttftMsP50, 200, 'only the finished session contributes');
});

test('per-model ttftMsP50 surfaces in the snapshot models array', () => {
  reset();
  const t = Date.now() - 10000;
  mk({ model: 'claude', status: 'done', startedAt: t, firstTokenAt: t + 120, finalTps: 40, endedAt: Date.now() });
  mk({ model: 'gpt-5', status: 'done', startedAt: t, firstTokenAt: t + 400, finalTps: 50, endedAt: Date.now() });
  const snap = sessions.getSessionsSnapshot();
  const claude = snap.aggregate.models.find((m) => m.model === 'claude');
  const gpt5 = snap.aggregate.models.find((m) => m.model === 'gpt-5');
  assert.equal(claude.ttftMsP50, 120);
  assert.equal(gpt5.ttftMsP50, 400);
});

test('session history evicts completed records without evicting active records', () => {
  reset();
  withLimits({ sessionHistory: 2 }, () => {
    const first = createAndComplete('first');
    const second = createAndComplete('second');
    const active = sessions.createSession({ model: 'm', stream: true, groupKey: 'active' });
    const third = createAndComplete('third');
    assert.equal(state.sessions.has(first.id), false);
    assert.equal(state.sessions.has(second.id), true);
    assert.equal(state.sessions.has(active.id), true);
    assert.equal(state.sessions.has(third.id), true);
  });
  reset();
});

test('history eviction counts only successful done sessions', () => {
  reset();
  withLimits({ sessionHistory: 1 }, () => {
    const first = createAndComplete('first');
    const failed = sessions.createSession({ model: 'm', stream: true, groupKey: 'failed' });
    sessions.finalizeSession(failed, 'error');
    const second = createAndComplete('second');
    assert.equal(state.sessions.has(first.id), false);
    assert.equal(state.sessions.has(second.id), true);
    assert.equal(state.sessions.has(failed.id), true);
    assert.equal(state.groupSummaries.has('failed'), false);
  });
  reset();
});

test('active session records are retained beyond the history budget', () => {
  reset();
  withLimits({ sessionHistory: 2 }, () => {
    const records = [];
    for (let i = 0; i < 100; i++) records.push(sessions.createSession({ model: 'm', stream: true, groupKey: 'g' + i }));
    assert.equal(state.sessions.size, 100);
    assert.equal(state.sessions.has(records[0].id), true);
    assert.equal(state.sessions.has(records[99].id), true);
  });
  reset();
});

test('group summaries and learned model telemetry honor configured cardinality budgets', () => {
  reset();
  withLimits({ groupSummaries: 2, modelRatios: 3, seenModels: 4 }, () => {
    for (let i = 0; i <= 2; i++) sessions.updateGroupSummary({ groupKey: 'g' + i, model: 'm', completionTokens: 1, cachedTokens: 0, promptTokens: 0, bytes: 1, finalTps: null });
    for (let i = 0; i <= 3; i++) sessions.updateModelRatio('m' + i, 4, 1);
    for (let i = 0; i <= 4; i++) sessions.createSession({ model: 'seen-' + i, stream: true, groupKey: 'seen-g' + i });
    assert.ok(state.groupSummaries.size <= 2);
    assert.ok(state.groupSummaryTimers.size <= 2);
    assert.ok(state.modelCharRatio.size <= 3);
    assert.ok(state.seenModels.size <= 4);
  });
  reset();
});

test('completed-session timers stay within the configured history budget', () => {
  reset();
  withLimits({ sessionHistory: 2 }, () => {
    for (let i = 0; i < 20; i++) createAndComplete('timer-' + i);
    assert.ok(state.sessions.size <= 2);
    assert.ok(state.sessionTimers.size <= 2);
  });
  reset();
});

function createAndComplete(groupKey) {
  const record = sessions.createSession({ model: 'm', stream: true, groupKey });
  sessions.finalizeSession(record, 'done');
  return record;
}
