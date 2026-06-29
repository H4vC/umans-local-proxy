'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { fnv1a, fnv1a32, fnv1aMixNum } = require('../lib/hash');
const { parseDuration, parseListenAddr, cleanKeys, fileProxyApiKeys, readConfigFile, isLoopbackHost } = require('../lib/config');
const { snapReasoningLevel, enrichModelsWithReasoning, REASONING_RANK } = require('../lib/reasoning');
const { firstNumber, concurrencyHardLimit, percentValue, burstQuota, concurrencyQuotaLimit, applyOverride, extractThrottle, getEffectiveConcurrency, acquireThrottleSlot, releaseThrottleSlot } = require('../lib/concurrency');
const { canonicalMessage, messageHash, chainHash } = require('../lib/coalesce');
const { authorized, wsUpgradeAllowed, isLoopback } = require('../lib/auth');
const state = require('../lib/state');
const { readBody } = require('../lib/http');
const { PassThrough } = require('node:stream');
const fs = require('node:fs');
const os = require('node:os');
const nodePath = require('node:path');

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

test('fnv1a32 matches fnv1a output for same input', () => {
  assert.strictEqual(fnv1a32('hello').toString(16).padStart(8, '0'), fnv1a('hello'));
});

test('fnv1a32 returns uint32', () => {
  const h = fnv1a32('test');
  assert.ok(Number.isInteger(h) && h >= 0 && h <= 0xffffffff);
});

test('fnv1aMixNum is deterministic', () => {
  const seed = fnv1a32('model');
  const msgHash = fnv1a32('hello');
  assert.strictEqual(fnv1aMixNum(seed, msgHash), fnv1aMixNum(seed, msgHash));
});

test('fnv1aMixNum differs on changed seed', () => {
  const h1 = fnv1a32('msg');
  assert.notStrictEqual(fnv1aMixNum(fnv1a32('a'), h1), fnv1aMixNum(fnv1a32('b'), h1));
});

test('fnv1aMixNum differs on changed hash', () => {
  const seed = fnv1a32('model');
  assert.notStrictEqual(fnv1aMixNum(seed, fnv1a32('a')), fnv1aMixNum(seed, fnv1a32('b')));
});

test('fnv1aMixNum returns uint32', () => {
  const h = fnv1aMixNum(0, 0);
  assert.ok(Number.isInteger(h) && h >= 0 && h <= 0xffffffff);
});

// ---- coalesce: numeric chain hashing (L4) ----

test('chainHash is deterministic', () => {
  const msgs = [{ role: 'user', content: 'hello' }];
  assert.strictEqual(chainHash('m', msgs), chainHash('m', msgs));
});

test('chainHash differs on changed model', () => {
  const msgs = [{ role: 'user', content: 'hi' }];
  assert.notStrictEqual(chainHash('m1', msgs), chainHash('m2', msgs));
});

test('chainHash differs on changed message', () => {
  const m1 = [{ role: 'user', content: 'hello' }];
  const m2 = [{ role: 'user', content: 'world' }];
  assert.notStrictEqual(chainHash('m', m1), chainHash('m', m2));
});

test('chainHash extends via fromChain', () => {
  const prefix = [{ role: 'user', content: 'q1' }];
  const full = [...prefix, { role: 'assistant', content: 'a1' }, { role: 'user', content: 'q2' }];
  const prefixChain = chainHash('m', prefix);
  const fromChain = chainHash('m', full.slice(prefix.length), prefixChain);
  const direct = chainHash('m', full);
  assert.strictEqual(fromChain, direct);
});

test('chainHash prefix differs from full', () => {
  const prefix = [{ role: 'user', content: 'q1' }];
  const full = [...prefix, { role: 'assistant', content: 'a1' }];
  assert.notStrictEqual(chainHash('m', prefix), chainHash('m', full));
});

test('messageHash returns uint32', () => {
  const h = messageHash({ role: 'user', content: 'x' });
  assert.ok(Number.isInteger(h) && h >= 0 && h <= 0xffffffff);
});
test('messageHash skips cache for large serialized messages', () => {
  const originalCache = state.messageHashCache;
  state.messageHashCache = new Map();
  try {
    const h = messageHash({ role: 'user', content: 'x'.repeat(9000) });
    assert.ok(Number.isInteger(h) && h >= 0 && h <= 0xffffffff);
    assert.strictEqual(state.messageHashCache.size, 0);
  } finally {
    state.messageHashCache = originalCache;
  }
});

test('readBody destroys oversized uploads', async () => {
  const originalMax = state.MAX_BODY_SIZE;
  state.MAX_BODY_SIZE = 4;
  const req = new PassThrough();
  try {
    const body = readBody(req);
    req.write(Buffer.alloc(5));
    await assert.rejects(body, /request body too large/);
    assert.strictEqual(req.destroyed, true);
  } finally {
    state.MAX_BODY_SIZE = originalMax;
  }
});

test('canonicalMessage normalizes null content to empty', () => {
  assert.deepStrictEqual(canonicalMessage({ role: 'user', content: null }), { role: 'user', content: '' });
});

test('canonicalMessage flattens assistant array content to joined text', () => {
  // proxy captured the text string; client replays text+thinking as an array
  const proxy = canonicalMessage({ role: 'assistant', content: 'hello' });
  const client = canonicalMessage({ role: 'assistant', content: [
    { type: 'thinking', thinking: 'secret', signature: 'sig' },
    { type: 'text', text: 'hello' },
  ] });
  assert.deepStrictEqual(proxy, client);
  assert.deepStrictEqual(proxy, { role: 'assistant', content: 'hello' });
});

test('canonicalMessage drops assistant tool_use blocks, keeps text', () => {
  const got = canonicalMessage({ role: 'assistant', content: [
    { type: 'text', text: 'calling foo' },
    { type: 'tool_use', id: 'c1', name: 'foo', input: { x: 1 } },
  ] });
  assert.deepStrictEqual(got, { role: 'assistant', content: 'calling foo' });
});

test('canonicalMessage keeps full stringify for non-assistant array content (vision)', () => {
  const blocks = [{ type: 'text', text: 'see this' }, { type: 'image_url', image_url: { url: 'data:…' } }];
  assert.deepStrictEqual(canonicalMessage({ role: 'user', content: blocks }), { role: 'user', content: JSON.stringify(blocks) });
});

test('assistant array and string with same text coalesce in chainHash', () => {
  const model = 'm';
  const user = { role: 'user', content: 'q' };
  const viaString = chainHash(model, [user, { role: 'assistant', content: 'a' }]);
  const viaArray = chainHash(model, [user, { role: 'assistant', content: [
    { type: 'thinking', thinking: 'x' }, { type: 'text', text: 'a' },
  ] }]);
  assert.strictEqual(viaString, viaArray);
});

// ---- auth: constant-time comparison (L3) ----

test('authorized returns true when no proxy keys configured', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, proxyApiKeys: [] };
  try { assert.strictEqual(authorized({}, new URL('http://localhost/')), true); }
  finally { require('../lib/state').config = orig; }
});

test('authorized matches valid key', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    const req = { headers: { 'x-api-key': 'secret-key' } };
    assert.strictEqual(authorized(req, new URL('http://localhost/')), true);
  } finally { require('../lib/state').config = orig; }
});

test('authorized rejects invalid key', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    const req = { headers: { 'x-api-key': 'wrong' } };
    assert.strictEqual(authorized(req, new URL('http://localhost/')), false);
  } finally { require('../lib/state').config = orig; }
});

test('authorized accepts bearer token', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    const req = { headers: { authorization: 'Bearer secret-key' } };
    assert.strictEqual(authorized(req, new URL('http://localhost/')), true);
  } finally { require('../lib/state').config = orig; }
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

test('snapReasoningLevel maps xhigh to highest declared level when its name is unknown', () => {
  // 'ultra' is not in REASONING_RANK; it must still receive a max intent
  // because it is the highest declared level (last by position).
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['none', 'high', 'ultra'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'xhigh'), 'ultra');
  assert.strictEqual(snapReasoningLevel('m1', 'max'), 'ultra');
});

test('snapReasoningLevel snaps to an interpolated unknown intermediate level', () => {
  // 'turbo' sits between 'low' and 'high'; a 'medium' request snaps up to it.
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'turbo', 'high'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'medium'), 'turbo');
});

test('snapReasoningLevel maps max to the only declared level when it is unknown', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'ultra'], can_disable: true } } } });
  assert.strictEqual(snapReasoningLevel('m1', 'xhigh'), 'ultra');
});

// ---- reasoning: enrichModelsWithReasoning ----

test('enrichModelsWithReasoning adds reasoning when supported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: true, levels: ['low', 'high'], can_disable: true, default_level: 'low' } } } });
  const result = enrichModelsWithReasoning([{ id: 'm1', object: 'model' }]);
  assert.strictEqual(result[0].reasoning.supported, true);
  assert.deepStrictEqual(result[0].reasoning.levels, ['low', 'high']);
  assert.deepStrictEqual(result[0].supported_endpoint_types, ['openai']);
});

test('enrichModelsWithReasoning advertises openai endpoint even when reasoning unsupported', () => {
  setModelInfo({ 'm1': { capabilities: { reasoning: { supported: false } } } });
  const result = enrichModelsWithReasoning([{ id: 'm1', object: 'model' }]);
  assert.strictEqual(result[0].reasoning, undefined);
  assert.deepStrictEqual(result[0].supported_endpoint_types, ['openai']);
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
  assert.strictEqual(percentValue(NaN, undefined, null), null);
});
test('percentValue skips null and returns next valid', () => {
  assert.strictEqual(percentValue(null, 50), 0.5);
});

// ---- concurrency: burstQuota ----

test('burstQuota returns 1 (full) when no remaining field', () => {
  assert.strictEqual(burstQuota({}), 1);
});

test('burstQuota reads burst_remaining_pct', () => {
  assert.strictEqual(burstQuota({ burst_remaining_pct: 0.7 }), 0.7);
});

test('burstQuota ignores burst_pct (hard-limit field, not quota)', () => {
  assert.strictEqual(burstQuota({ burst_pct: 0.5 }), 1);
});

test('burstQuota ignores burst_percent (hard-limit field, not quota)', () => {
  assert.strictEqual(burstQuota({ burst_percent: 80 }), 1);
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

test('concurrencyQuotaLimit uses hard limit via burst_pct when no live quota', () => {
  // soft=10, burst_pct=0.5 → hard=15; no live remaining field → full burst → 15
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, burst_pct: 0.5 }), 15);
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

test('extractThrottle clamps boxed accounts to soft concurrency', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, overrideConcurrency: 0 };
  try {
    const boxedUntil = new Date(Date.now() + 60000).toISOString();
    const throttle = extractThrottle({
      limits: { concurrency: { limit: 4, hard_cap: 8, burst_pct: 1 } },
      usage: { concurrent_sessions: 0, priority: { low: true, boxed_until: boxedUntil } },
    });
    assert.strictEqual(throttle.limit, 4);
    assert.strictEqual(throttle.softLimit, 4);
    assert.strictEqual(throttle.quotaLimit, 8);
    assert.strictEqual(throttle.boxed, true);
    assert.strictEqual(throttle.boxedUntil, boxedUntil);
  } finally {
    require('../lib/state').config = orig;
  }
});

test('getEffectiveConcurrency clamps active boxed cache to soft concurrency', () => {
  const origConfig = require('../lib/state').config;
  const origCache = require('../lib/state').concurrencyCache;
  const origCooldown = require('../lib/state').burstDisabledUntil;
  require('../lib/state').config = { ...origConfig, overrideConcurrency: 0 };
  require('../lib/state').burstDisabledUntil = 0;
  try {
    const boxedMs = Date.now() + 60000;
    require('../lib/state').concurrencyCache = { concurrent: 1, limit: 8, softLimit: 4, boxedUntil: boxedMs, time: Date.now() };
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.limit, 4);
    assert.strictEqual(effective.softLimit, 4);
    assert.strictEqual(effective.boxed, true);
    assert.strictEqual(effective.boxedUntil, new Date(boxedMs).toISOString());
  } finally {
    require('../lib/state').config = origConfig;
    require('../lib/state').concurrencyCache = origCache;
    require('../lib/state').burstDisabledUntil = origCooldown;
  }
});

test('extractThrottle clamps to soft while burst cooldown is active', () => {
  const origConfig = require('../lib/state').config;
  const origCooldown = require('../lib/state').burstDisabledUntil;
  require('../lib/state').config = { ...origConfig, overrideConcurrency: 0 };
  const until = Date.now() + 60000;
  require('../lib/state').burstDisabledUntil = until;
  try {
    const throttle = extractThrottle({
      limits: { concurrency: { limit: 4, hard_cap: 8, burst_pct: 1 } },
      usage: { concurrent_sessions: 0 },
    });
    assert.strictEqual(throttle.limit, 4);
    assert.strictEqual(throttle.softLimit, 4);
    assert.strictEqual(throttle.quotaLimit, 8);
    assert.strictEqual(throttle.burstCooldown, true);
    assert.strictEqual(throttle.burstCooldownUntil, new Date(until).toISOString());
  } finally {
    require('../lib/state').config = origConfig;
    require('../lib/state').burstDisabledUntil = origCooldown;
  }
});

test('getEffectiveConcurrency clamps to soft while burst cooldown is active', () => {
  const origConfig = require('../lib/state').config;
  const origCache = require('../lib/state').concurrencyCache;
  const origCooldown = require('../lib/state').burstDisabledUntil;
  require('../lib/state').config = { ...origConfig, overrideConcurrency: 0 };
  require('../lib/state').burstDisabledUntil = Date.now() + 60000;
  try {
    require('../lib/state').concurrencyCache = { concurrent: 1, limit: 8, softLimit: 4, boxedUntil: null, time: Date.now() };
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.limit, 4);
    assert.strictEqual(effective.softLimit, 4);
    assert.strictEqual(effective.burstCooldown, true);
    assert.ok(effective.burstCooldownUntil);
  } finally {
    require('../lib/state').config = origConfig;
    require('../lib/state').concurrencyCache = origCache;
    require('../lib/state').burstDisabledUntil = origCooldown;
  }
});

test('acquireThrottleSlot releases a woken-then-aborted slot (no leak)', async () => {
  const s = require('../lib/state');
  const orig = {
    config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache,
    activeRequests: s.activeRequests, queuedRequests: s.queuedRequests,
    throttleWaiters: s.throttleWaiters, burstDisabledUntil: s.burstDisabledUntil,
    refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer,
  };
  s.config = { ...s.config, overrideConcurrency: 0, apiKey: '' };
  s.concurrencyCache = { concurrent: 0, limit: 1, softLimit: 1, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() }; // refreshUsageSoon is a no-op (no network)
  s.activeRequests = 1; // at the limit → next acquire queues
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.burstDisabledUntil = 0;
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  try {
    const controller = new AbortController();
    // Queues (waiter pushed). releaseThrottleSlot wakes it (pre-increments
    // activeRequests) and we abort before the await resumes — the exact
    // woken-then-aborted race that used to leak a slot.
    const p = acquireThrottleSlot({}, controller.signal);
    releaseThrottleSlot();
    controller.abort();
    await assert.rejects(p, /aborted/);
    assert.strictEqual(s.activeRequests, 0, 'woken-then-aborted slot must be released');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
    s.throttleWaiters = orig.throttleWaiters; s.burstDisabledUntil = orig.burstDisabledUntil;
    s.refreshUsageInFlight = orig.refreshUsageInFlight; s.refreshUsageTimer = orig.refreshUsageTimer;
  }
});

// ---- B6: authorized is header-only (no ?key= query param) ----

test('authorized ignores ?key= query param — header only', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    assert.strictEqual(authorized({ headers: {} }), false); // no header → rejected
    assert.strictEqual(authorized({ headers: { 'x-api-key': 'secret-key' } }), true);
  } finally { state.config = orig; }
});

// ---- B1: wsUpgradeAllowed (CSWSH defense) ----

test('wsUpgradeAllowed accepts a same-origin browser (Origin authority === Host)', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: [] };
  try {
    const req = { headers: { origin: 'http://127.0.0.1:8084', host: '127.0.0.1:8084' }, socket: { remoteAddress: '127.0.0.1' } };
    assert.strictEqual(wsUpgradeAllowed(req), true);
  } finally { state.config = orig; }
});

test('wsUpgradeAllowed rejects a cross-origin browser (CSWSH)', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: [] };
  try {
    const req = { headers: { origin: 'https://evil.com', host: '127.0.0.1:8084' }, socket: { remoteAddress: '127.0.0.1' } };
    assert.strictEqual(wsUpgradeAllowed(req), false);
  } finally { state.config = orig; }
});

test('wsUpgradeAllowed trusts a no-Origin loopback client (local non-browser)', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: [] };
  try {
    const req = { headers: {}, socket: { remoteAddress: '127.0.0.1' } };
    assert.strictEqual(wsUpgradeAllowed(req), true);
  } finally { state.config = orig; }
});

test('wsUpgradeAllowed rejects a no-Origin non-loopback client without a key', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    const req = { headers: {}, socket: { remoteAddress: '203.0.113.5' } };
    assert.strictEqual(wsUpgradeAllowed(req), false);
  } finally { state.config = orig; }
});

test('wsUpgradeAllowed accepts a no-Origin non-loopback client with a valid header key', () => {
  const orig = state.config;
  state.config = { ...orig, proxyApiKeys: ['secret-key'] };
  try {
    const req = { headers: { 'x-api-key': 'secret-key' }, socket: { remoteAddress: '203.0.113.5' } };
    assert.strictEqual(wsUpgradeAllowed(req), true);
  } finally { state.config = orig; }
});

// ---- B2: readConfigFile (strict at boot) ----

test('readConfigFile returns {} for a missing file', () => {
  assert.deepStrictEqual(readConfigFile(nodePath.join(os.tmpdir(), 'umans-missing-' + Date.now() + '.json')), {});
});

test('readConfigFile throws on a corrupt (unparseable) file', () => {
  const file = nodePath.join(os.tmpdir(), 'umans-corrupt-' + Date.now() + '.json');
  fs.writeFileSync(file, '{ not valid json');
  try {
    assert.throws(() => readConfigFile(file), /corrupt/);
  } finally { try { fs.rmSync(file, { force: true }); } catch {} }
});

// ---- B3: isLoopbackHost (boot guard) ----

test('isLoopbackHost identifies loopback listen hosts', () => {
  assert.strictEqual(isLoopbackHost('127.0.0.1'), true);
  assert.strictEqual(isLoopbackHost('::1'), true);
  assert.strictEqual(isLoopbackHost('localhost'), true);
  assert.strictEqual(isLoopbackHost('0.0.0.0'), false);
  assert.strictEqual(isLoopbackHost('192.168.1.5'), false);
});
