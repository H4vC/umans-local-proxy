'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { fnv1a } = require('../lib/hash');
const { parseDuration, parseListenAddr, cleanKeys, fileProxyApiKeys } = require('../lib/config');
const { snapReasoningLevel, enrichModelsWithReasoning, REASONING_RANK } = require('../lib/reasoning');
const { firstNumber, concurrencyHardLimit, percentValue, burstQuota, concurrencyQuotaLimit, applyOverride } = require('../lib/concurrency');

// ---- hash ----

test('fnv1a returns 8-char hex', () => {
  assert.match(fnv1a('hello'), /^[0-9a-f]{8}$/);
});

test('fnv1a is deterministic', () => {
  assert.strictEqual(fnv1a('test'), fnv1a('test'));
});

test('fnv1a differs on different input', () => {
  assert.notStrictEqual(fnv1a('hello'), fnv1a('world'));
});

test('fnv1a empty string returns FNV offset basis', () => {
  assert.strictEqual(fnv1a(''), '811c9dc5');
});

// ---- config: parseDuration ----

test('parseDuration ms', () => {
  assert.strictEqual(parseDuration('30000ms'), 30000);
});

test('parseDuration s', () => {
  assert.strictEqual(parseDuration('30s'), 30000);
});

test('parseDuration m', () => {
  assert.strictEqual(parseDuration('15m'), 900000);
});

test('parseDuration h', () => {
  assert.strictEqual(parseDuration('1h'), 3600000);
});

test('parseDuration defaults to ms when no unit', () => {
  assert.strictEqual(parseDuration('5000'), 5000);
});

test('parseDuration case-insensitive', () => {
  assert.strictEqual(parseDuration('15M'), 900000);
});

test('parseDuration throws on invalid', () => {
  assert.throws(() => parseDuration('abc'), /REQUEST_TIMEOUT/);
});

// ---- config: parseListenAddr ----

test('parseListenAddr host:port', () => {
  assert.deepStrictEqual(parseListenAddr('127.0.0.1:8084'), { host: '127.0.0.1', port: 8084 });
});

test('parseListenAddr port only defaults to 127.0.0.1', () => {
  assert.deepStrictEqual(parseListenAddr('9000'), { host: '127.0.0.1', port: 9000 });
});

test('parseListenAddr empty returns default', () => {
  assert.deepStrictEqual(parseListenAddr(''), { host: '127.0.0.1', port: 8084 });
});

test('parseListenAddr rejects port 0', () => {
  assert.throws(() => parseListenAddr('0.0.0.0:0'), /integer from 1/);
});

test('parseListenAddr rejects port > 65535', () => {
  assert.throws(() => parseListenAddr('0.0.0.0:99999'), /integer from 1/);
});

// ---- config: cleanKeys / fileProxyApiKeys ----

test('cleanKeys dedupes and trims', () => {
  assert.deepStrictEqual(cleanKeys([' a ', 'a', 'b', '']), ['a', 'b']);
});

test('cleanKeys handles objects with null', () => {
  assert.deepStrictEqual(cleanKeys([null, undefined, 'ok']), ['ok']);
});

test('fileProxyApiKeys extracts strings from array', () => {
  assert.deepStrictEqual(fileProxyApiKeys({ API_KEYS: ['key1', 'key2'] }), ['key1', 'key2']);
});

test('fileProxyApiKeys extracts .key from objects', () => {
  assert.deepStrictEqual(fileProxyApiKeys({ API_KEYS: [{ key: 'secret' }, 'plain'] }), ['secret', 'plain']);
});

test('fileProxyApiKeys empty when no API_KEYS', () => {
  assert.deepStrictEqual(fileProxyApiKeys({}), []);
});

// ---- reasoning: snapReasoningLevel ----
// Requires state.modelInfoCache to be populated.

function setModelInfo(info) {
  const state = require('../lib/state');
  state.modelInfoCache = { data: info, time: Date.now() };
}

test('snapReasoningLevel returns null when model unknown', () => {
  setModelInfo({});
  assert.strictEqual(snapReasoningLevel('unknown-model', 'high'), null);
});

test('snapReasoningLevel returns null when reasoning not supported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: false } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'high'), null);
});

test('snapReasoningLevel preserves already-supported level', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'medium', 'high'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'high'), 'high');
});

test('snapReasoningLevel snaps UP to nearest supported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'high'], can_disable: true } } } });
  // 'medium' (rank 3) snaps up to 'high' (rank 4) since 'medium' isn't available
  assert.strictEqual(snapReasoningLevel('m1', 'medium'), 'high');
});

test('snapReasoningLevel clamps to highest when nothing reaches requested', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'medium'], can_disable: true } } } });
  // 'max' (rank 5) snaps down to 'medium' (rank 3) — highest available
  assert.strictEqual(snapReasoningLevel('m1', 'max'), 'medium');
});

test('snapReasoningLevel with levels + can_disable false: none snaps up to low', () => {
  // With a levels list, the filter drops 'none' (rank 0) because can_disable
  // is false, so 'none' snaps UP to the nearest available: 'low'.
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'medium', 'high'], can_disable: false } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'none'), 'low');
});

test('snapReasoningLevel maps xhi/xhigh to max via rank', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['none', 'low', 'medium', 'high', 'max'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'xhi'), 'max');
  assert.strictEqual(snapReasoningLevel('m1', 'xhigh'), 'max');
});

test('snapReasoningLevel returns null for genuinely unknown effort', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'high'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'ultra'), null);
});

test('snapReasoningLevel with no levels list: pass through', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'high'), 'high');
});

test('snapReasoningLevel with no levels list: drop none when can_disable false', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, can_disable: false } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'off'), null);
});

// ---- reasoning: enrichModelsWithReasoning ----

test('enrichModelsWithReasoning adds reasoning when supported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'high'], can_disable: true, default_level: 'low' } } } });
  const result = enrichModelsWithReasoning([{ id: 'm1', object: 'model' }]);
  assert.strictEqual(result[0].reasoning.supported, true);
  assert.deepStrictEqual(result[0].reasoning.levels, ['low', 'high']);
});

test('enrichModelsWithReasoning leaves model untouched when not supported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: false } } } });
  const result = enrichModelsWithReasoning([{ id: 'm1', object: 'model' }]);
  assert.strictEqual(result[0].reasoning, undefined);
});

// ---- concurrency: firstNumber ----

test('firstNumber returns first finite positive', () => {
  assert.strictEqual(firstNumber(0, null, undefined, 5, 10), 5);
});

test('firstNumber returns null when none valid', () => {
  assert.strictEqual(firstNumber(0, null, undefined, -1, NaN), null);
});

// ---- concurrency: concurrencyHardLimit ----

test('concurrencyHardLimit returns hard_cap when present', () => {
  assert.strictEqual(concurrencyHardLimit({ limit: 10, hard_cap: 20 }), 20);
});

test('concurrencyHardLimit returns soft + burst', () => {
  assert.strictEqual(concurrencyHardLimit({ limit: 10, burst: 5 }), 15);
});

test('concurrencyHardLimit returns soft when no burst', () => {
  assert.strictEqual(concurrencyHardLimit({ limit: 10 }), 10);
});

test('concurrencyHardLimit returns soft * (1 + burst_pct)', () => {
  assert.strictEqual(concurrencyHardLimit({ limit: 10, burst_pct: 0.5 }), 15);
});

// ---- concurrency: percentValue ----

test('percentValue normalizes 50 to 0.5', () => {
  assert.strictEqual(percentValue(50), 0.5);
});

test('percentValue passes through 0.5', () => {
  assert.strictEqual(percentValue(0.5), 0.5);
});

test('percentValue clamps to 1', () => {
  assert.strictEqual(percentValue(150), 1);
});

test('percentValue skips negative, returns null', () => {
  assert.strictEqual(percentValue(-10), null);
});

test('percentValue returns null for no valid input', () => {
  // null coerces to 0 which IS finite and >= 0, so it returns 0 — not null.
  // Only NaN/undefined are truly invalid.
  assert.strictEqual(percentValue(NaN, undefined), null);
});

// ---- concurrency: burstQuota ----

test('burstQuota returns 0 when no fields present', () => {
  assert.strictEqual(burstQuota({}), 0);
});

test('burstQuota reads burst_remaining_pct', () => {
  assert.strictEqual(burstQuota({ burst_remaining_pct: 0.7 }), 0.7);
});

test('burstQuota reads burst_percent (normalized)', () => {
  assert.strictEqual(burstQuota({ burst_percent: 80 }), 0.8);
});

// ---- concurrency: concurrencyQuotaLimit ----

test('concurrencyQuotaLimit returns hard when no soft', () => {
  assert.strictEqual(concurrencyQuotaLimit({ hard_cap: 20 }), 20);
});

test('concurrencyQuotaLimit returns soft when no hard', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10 }), 10);
});

test('concurrencyQuotaLimit returns hard when hard <= soft', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, hard_cap: 8 }), 8);
});

test('concurrencyQuotaLimit interpolates with burst quota', () => {
  // soft=10, hard=20, burst_quota=0.5 → 10 + floor(10*0.5) = 15
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, hard_cap: 20, burst_remaining_pct: 0.5 }), 15);
});

// ---- concurrency: applyOverride ----

test('applyOverride no override returns as-is', () => {
  assert.deepStrictEqual(applyOverride(10, 5, 0), { limit: 10, softLimit: 5, overridden: false });
});

test('applyOverride caps limit below override', () => {
  const result = applyOverride(10, 5, 8);
  assert.strictEqual(result.limit, 8);
  assert.strictEqual(result.softLimit, 5);
  assert.strictEqual(result.overridden, true);
});

test('applyOverride with null apiLimit uses override directly', () => {
  const result = applyOverride(null, null, 5);
  assert.strictEqual(result.limit, 5);
  assert.strictEqual(result.softLimit, null);
  assert.strictEqual(result.overridden, true);
});

test('applyOverride override higher than apiLimit is not overridden', () => {
  const result = applyOverride(10, 5, 20);
  assert.strictEqual(result.limit, 10);
  assert.strictEqual(result.overridden, false);
});
