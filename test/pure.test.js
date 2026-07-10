'use strict';
require('./fetch-guard');

const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const { fnv1a, fnv1a32, fnv1aMixNum } = require('../lib/hash');
const { parseDuration, parseWebsearchProvider, parseRequestLogging, parseListenAddr, cleanKeys, fileProxyApiKeys, readConfigFile, isLoopbackHost, normalizeSessionCookie } = require('../lib/config');
const { snapReasoningLevel, enrichModelsWithReasoning, REASONING_RANK } = require('../lib/reasoning');
const { firstNumber, concurrencyHardLimit, percentValue, burstQuota, concurrencyQuotaLimit, applyOverride, extractThrottle, fetchUmansUsage, getEffectiveConcurrency, canStart, acquireThrottleSlot, releaseThrottleSlot, notifyUpstream429, parseRetryAfter, ThrottleQueueFullError, MAX_THROTTLE_WAITERS, BURST_COOLDOWN_FILE, RELEASE_COOLDOWN_MS, PHANTOM_WINDOW, coolingDownNow, currentPhantomEstimate, wakeThrottleWaiters } = require('../lib/concurrency');
const { canonicalMessage, messageHash, chainHash, resolveGroupKey, storeStateKey } = require('../lib/coalesce');
const { authorized, wsUpgradeAllowed, isLoopback, safeHeaders, requiresProxyAuth } = require('../lib/auth');
const { createSession, finalizeSession } = require('../lib/sessions');
const { projectStatus } = require('../lib/upstream');
const { decide429Backoff, CAP_HEALTH_BASELINE_FILE } = require('../lib/cap-health');
const state = require('../lib/state');
const { readBody } = require('../lib/http');
const { PassThrough } = require('node:stream');
const fs = require('node:fs');
const os = require('node:os');
const nodePath = require('node:path');

// Reset shared admission state because state.js is a module-level singleton.
beforeEach(() => {
  state.releaseCooldowns = [];
  state.phantomSamples = [];
  clearTimeout(state.cooldownWakeTimer);
  state.cooldownWakeTimer = null;
  state.cooldownWakeAt = 0;
  clearTimeout(state.retryAfterWakeTimer);
  state.retryAfterWakeTimer = null;
  state.retryAfterUntil = 0;
});

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

// ---- config: parseWebsearchProvider ----

test('parseWebsearchProvider accepts native/exa/none', () => {
  assert.strictEqual(parseWebsearchProvider('native'), 'native');
  assert.strictEqual(parseWebsearchProvider('exa'), 'exa');
  assert.strictEqual(parseWebsearchProvider('none'), 'none');
});

test('parseWebsearchProvider defaults to none on empty/missing', () => {
  assert.strictEqual(parseWebsearchProvider(''), 'none');
  assert.strictEqual(parseWebsearchProvider(undefined), 'none');
  assert.strictEqual(parseWebsearchProvider(null), 'none');
});

test('parseWebsearchProvider is case-insensitive and trims', () => {
  assert.strictEqual(parseWebsearchProvider(' NATIVE '), 'native');
  assert.strictEqual(parseWebsearchProvider('Exa'), 'exa');
});

test('parseRequestLogging accepts off/basic/verbose', () => {
  assert.strictEqual(parseRequestLogging('off'), 'off');
  assert.strictEqual(parseRequestLogging('basic'), 'basic');
  assert.strictEqual(parseRequestLogging('verbose'), 'verbose');
});

test('parseRequestLogging defaults to off on empty/missing', () => {
  assert.strictEqual(parseRequestLogging(''), 'off');
  assert.strictEqual(parseRequestLogging(undefined), 'off');
  assert.strictEqual(parseRequestLogging(null), 'off');
});

test('parseRequestLogging is case-insensitive and trims', () => {
  assert.strictEqual(parseRequestLogging(' BASIC '), 'basic');
  assert.strictEqual(parseRequestLogging('Verbose'), 'verbose');
});

test('parseRequestLogging throws on unknown value', () => {
  assert.throws(() => parseRequestLogging('chatty'), /REQUEST_LOGGING/);
});

 // ---- config: parseListenProvider ----

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

// ---- config: normalizeSessionCookie (paste just the token) ----

test('normalizeSessionCookie wraps a bare token with the cookie name', () => {
  assert.strictEqual(normalizeSessionCookie('eyJabc.def.sig'), '__Secure-authjs.session-token=eyJabc.def.sig');
});

test('normalizeSessionCookie passes a full name=value header through as-is', () => {
  assert.strictEqual(normalizeSessionCookie('__Secure-authjs.session-token=eyJabc.def.sig'), '__Secure-authjs.session-token=eyJabc.def.sig');
});

test('normalizeSessionCookie passes a full multi-cookie header through as-is', () => {
  assert.strictEqual(normalizeSessionCookie('__Secure-authjs.session-token=eyJabc.def.sig; other=foo'), '__Secure-authjs.session-token=eyJabc.def.sig; other=foo');
});

test('normalizeSessionCookie strips a leading "Cookie:" prefix', () => {
  assert.strictEqual(normalizeSessionCookie('Cookie: __Secure-authjs.session-token=eyJabc.def.sig'), '__Secure-authjs.session-token=eyJabc.def.sig');
});

test('normalizeSessionCookie returns empty for empty/whitespace/null input', () => {
  assert.strictEqual(normalizeSessionCookie(''), '');
  assert.strictEqual(normalizeSessionCookie('   '), '');
  assert.strictEqual(normalizeSessionCookie(null), '');
});

test('normalizeSessionCookie discards a token with a copy-paste ellipsis (U+2026)', () => {
  assert.strictEqual(normalizeSessionCookie('eyJabc\u2026def.sig'), '');
});

test('normalizeSessionCookie discards a header with smart quotes / NBSP', () => {
  assert.strictEqual(normalizeSessionCookie('__Secure-authjs.session-token=eyJ\u2019abc'), '');
  assert.strictEqual(normalizeSessionCookie('foo=bar\u00A0baz'), '');
});

test('normalizeSessionCookie keeps structural chars (; = space) in a multi-cookie header', () => {
  assert.strictEqual(normalizeSessionCookie('a=b; c=d e'), 'a=b; c=d e');
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

test('concurrencyQuotaLimit returns null when no soft', () => {
  assert.strictEqual(concurrencyQuotaLimit({ hard_cap: 20 }), null);
});

test('concurrencyQuotaLimit returns soft when no hard', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10 }), 10);
});

test('concurrencyQuotaLimit returns hard when hard <= soft', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, hard_cap: 8 }), 8);
});

test('concurrencyQuotaLimit spends burst quota up to hard cap', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, hard_cap: 20, burst_remaining_pct: 0.5 }), 15);
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, hard_cap: 20, burst_remaining_pct: 1 }), 20);
});

test('concurrencyQuotaLimit uses full burst when no remaining quota field exists', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 10, burst_pct: 0.5 }), 15);
});

test('concurrencyQuotaLimit returns null when usage reports no positive max', () => {
  assert.strictEqual(concurrencyQuotaLimit({ limit: 0, hard_cap: 0 }), null);
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
    assert.strictEqual(throttle.quotaLimit, 8);
    assert.strictEqual(throttle.softLimit, 4);
    assert.strictEqual(throttle.limit, 4);
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
    assert.strictEqual(throttle.quotaLimit, 8);
    assert.strictEqual(throttle.softLimit, 4);
    assert.strictEqual(throttle.limit, 4);
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
    // Queues (waiter pushed). releaseThrottleSlot pushes a cooldown (the
    // released permit rests 2s) so wakeThrottleWaiters can't wake immediately
    // — held(0)+cooling(1) >= limit(1). Clear the cooldown and manually wake
    // to simulate the cooldown timer firing, then abort before the await
    // resumes — the exact woken-then-aborted race that used to leak a slot.
    const p = acquireThrottleSlot({}, controller.signal);
    releaseThrottleSlot();
    s.releaseCooldowns = []; // expire the cooling permit so wake finds room
    wakeThrottleWaiters();   // pre-increments activeRequests to 1
    controller.abort();      // abort before the await resumes
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

// ---- C7: safeHeaders strips content-encoding only for decoded encodings ----

test('safeHeaders strips content-encoding + content-length for gzip (undici decodes)', () => {
  const h = safeHeaders([['content-encoding', 'gzip'], ['content-length', '45'], ['content-type', 'application/json']]);
  assert.strictEqual(h['content-encoding'], undefined);
  assert.strictEqual(h['content-length'], undefined);
  assert.strictEqual(h['content-type'], 'application/json');
});

test('safeHeaders keeps content-encoding for zstd (undici passes raw bytes through)', () => {
  const h = safeHeaders([['content-encoding', 'zstd'], ['content-length', '45'], ['content-type', 'application/json']]);
  assert.strictEqual(h['content-encoding'], 'zstd');
  assert.strictEqual(h['content-length'], undefined); // always stripped — the pipe is chunked
  assert.strictEqual(h['content-type'], 'application/json');
});

test('safeHeaders keeps content-encoding for unknown encodings (pass-through verbatim)', () => {
  const h = safeHeaders([['content-encoding', 'weird'], ['content-type', 'text/plain']]);
  assert.strictEqual(h['content-encoding'], 'weird');
});

// ---- C3: getEffectiveConcurrency collapses stale upstream concurrent ----

test('getEffectiveConcurrency prefers activeRequests when the usage cache is stale (C3)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, activeRequests: s.activeRequests };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.burstDisabledUntil = 0;
  // Stale cache: upstream reported 10 concurrent (all external), only 2 ours.
  s.concurrencyCache = { concurrent: 10, limit: 10, softLimit: 5, boxedUntil: null, time: Date.now() - 60000 };
  s.activeRequests = 2;
  try {
    assert.strictEqual(getEffectiveConcurrency().concurrent, 2, 'stale upstream concurrent collapses to activeRequests');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.burstDisabledUntil = orig.burstDisabledUntil; s.activeRequests = orig.activeRequests;
  }
});

test('getEffectiveConcurrency trusts upstream concurrent when the cache is fresh (C3)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, activeRequests: s.activeRequests };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.burstDisabledUntil = 0;
  s.concurrencyCache = { concurrent: 10, limit: 10, softLimit: 5, boxedUntil: null, time: Date.now() };
  s.activeRequests = 2;
  try {
    assert.strictEqual(getEffectiveConcurrency().concurrent, 10, 'fresh upstream concurrent is trusted');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.burstDisabledUntil = orig.burstDisabledUntil; s.activeRequests = orig.activeRequests;
  }
});

test('fetchUmansUsage caches top-level concurrent_sessions for admission', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = { config: s.config, usageCache: s.usageCache, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, burstDisabledUntil: s.burstDisabledUntil };
  global.fetch = async () => ({ ok: true, json: async () => ({ concurrent_sessions: 8, limits: { concurrency: { limit: 8 } } }) });
  s.config = { ...s.config, apiKey: 'test-key', upstreamBaseURL: 'https://example.test', overrideConcurrency: 0 };
  s.usageCache = { data: null, time: 0 };
  s.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  s.usageEverFetched = false;
  s.activeRequests = 0;
  s.burstDisabledUntil = 0;
  try {
    const data = await fetchUmansUsage({ force: true });
    const effective = getEffectiveConcurrency();
    assert.strictEqual(data.throttle.concurrent, 8, 'extractThrottle reads top-level concurrent_sessions');
    assert.strictEqual(effective.concurrent, 8, 'admission cache must store the same top-level concurrent_sessions');
    assert.strictEqual(canStart(effective), false, 'at-limit upstream concurrency must block a new local request');
  } finally {
    global.fetch = origFetch;
    s.config = orig.config; s.usageCache = orig.usageCache; s.concurrencyCache = orig.concurrencyCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests; s.burstDisabledUntil = orig.burstDisabledUntil;
  }
});

test('acquireThrottleSlot abandons a shared usage fetch when its caller aborts', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = {
    config: s.config, usageCache: s.usageCache, concurrencyCache: s.concurrencyCache,
    usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests,
    queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
  };
  let resolveFetch;
  global.fetch = () => new Promise((resolve) => { resolveFetch = resolve; });
  s.config = { ...s.config, apiKey: 'test-key', upstreamBaseURL: 'https://example.test', overrideConcurrency: 0 };
  s.usageCache = { data: null, time: 0 };
  s.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  s.usageEverFetched = false;
  s.activeRequests = 1;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  const controller = new AbortController();
  const acquire = acquireThrottleSlot({}, controller.signal);
  try {
    await new Promise((r) => setImmediate(r));
    controller.abort();
    await assert.rejects(acquire, /aborted/);
    assert.strictEqual(s.throttleWaiters.length, 0, 'caller must not become a waiter while shared usage is pending');
    assert.strictEqual(s.activeRequests, 1, 'caller must not alter an unrelated active lease');
  } finally {
    resolveFetch?.({ ok: true, json: async () => ({ usage: { concurrent_sessions: 1 }, limits: { concurrency: { limit: 1 } } }) });
    await new Promise((r) => setImmediate(r));
    global.fetch = origFetch;
    s.config = orig.config; s.usageCache = orig.usageCache; s.concurrencyCache = orig.concurrencyCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
  }
});

test('failed stale usage refresh wakes waiters to re-check admission', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = {
    config: s.config, usageCache: s.usageCache, concurrencyCache: s.concurrencyCache,
    usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests,
    queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
    refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer,
    burstDisabledUntil: s.burstDisabledUntil,
    phantomSamples: s.phantomSamples,
  };
  global.fetch = async () => { throw new Error('usage down'); };
  s.config = { ...s.config, apiKey: 'test-key', upstreamBaseURL: 'https://example.test', overrideConcurrency: 0 };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.concurrencyCache = { concurrent: 8, limit: 8, softLimit: 4, boxedUntil: null, time: Date.now() };
  // Seed a phantom sample so the gate shrinks to 0 and the initial acquire
  // queues. The new admission model uses held+cooling vs a phantom-absorbed
  // limit, not max(local, upstream) — without this sample the limit stays 8
  // and the request acquires immediately (no queue to wake from).
  s.phantomSamples = [{ observed: 8, local: 0 }];
  s.usageEverFetched = true;
  s.activeRequests = 0;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  s.burstDisabledUntil = 0;
  const controller = new AbortController();
  let acquired = false;
  const acquire = acquireThrottleSlot({}, controller.signal).then(() => { acquired = true; });
  try {
    await new Promise((r) => setImmediate(r));
    assert.strictEqual(s.throttleWaiters.length, 1, 'fresh at-limit upstream concurrency queues the request');

    s.concurrencyCache.time = Date.now() - 60000;
    await fetchUmansUsage({ force: true });
    await Promise.race([
      acquire,
      new Promise((_, reject) => setTimeout(() => reject(new Error('waiter was not woken')), 100)),
    ]);
    assert.strictEqual(acquired, true);
    assert.strictEqual(s.activeRequests, 1, 'wake pre-increments the acquired slot');
    releaseThrottleSlot();
  } finally {
    controller.abort();
    await acquire.catch(() => {});
    if (s.refreshUsageTimer) clearTimeout(s.refreshUsageTimer);
    global.fetch = origFetch;
    s.config = orig.config; s.usageCache = orig.usageCache; s.concurrencyCache = orig.concurrencyCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
    s.refreshUsageInFlight = orig.refreshUsageInFlight; s.refreshUsageTimer = orig.refreshUsageTimer;
    s.burstDisabledUntil = orig.burstDisabledUntil;
    s.phantomSamples = orig.phantomSamples;
  }
});

// ---- C2: canStart cold-start floor before the first /usage fetch ----

test('canStart admits only the cold-start floor before the first /usage fetch (C2)', () => {
  const s = require('../lib/state');
  const orig = { concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests };
  s.usageEverFetched = false;
  s.activeRequests = 0;
  try {
    // null limit + never fetched → admit at most COLD_START_FLOOR (1).
    assert.strictEqual(canStart({ limit: null, softLimit: null, concurrent: 0 }), true);  // 0 < 1
    s.activeRequests = 1;
    assert.strictEqual(canStart({ limit: null, softLimit: null, concurrent: 0 }), false); // 1 < 1 is false
  } finally {
    s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
  }
});

test('getEffectiveConcurrency hard-bounds broken post-fetch usage to 4', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, burstDisabledUntil: s.burstDisabledUntil };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.concurrencyCache = { concurrent: 0, limit: null, softLimit: null, boxedUntil: null, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 4;
  s.burstDisabledUntil = 0;
  try {
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.limit, 4);
    assert.strictEqual(canStart(effective), false);
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched;
    s.activeRequests = orig.activeRequests; s.burstDisabledUntil = orig.burstDisabledUntil;
  }
});

test('parallel local admission queues requests only up to the explicit bound', async () => {
  const s = require('../lib/state');
  const orig = {
    config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache,
    usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests,
    queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
    refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer,
    burstDisabledUntil: s.burstDisabledUntil,
  };
  s.config = { ...s.config, apiKey: '', overrideConcurrency: 0 };
  s.concurrencyCache = { concurrent: 0, limit: 4, softLimit: 4, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 0;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  s.burstDisabledUntil = 0;
  const controllers = Array.from({ length: 4 + MAX_THROTTLE_WAITERS + 4 }, () => new AbortController());
  const attempts = controllers.map((controller) => acquireThrottleSlot({}, controller.signal));
  // Observe immediate overload rejections as they occur so Node does not flag
  // intentionally rejected overflow promises as unhandled.
  const settled = attempts.map((attempt) => attempt.then(() => null, (err) => err));
  try {
    await new Promise((r) => setImmediate(r));
    assert.strictEqual(s.activeRequests, 4, 'only four slots may be acquired');
    assert.strictEqual(s.throttleWaiters.length, MAX_THROTTLE_WAITERS, 'only the bounded FIFO is retained');
    const overflow = await Promise.all(settled.slice(4 + MAX_THROTTLE_WAITERS));
    assert.ok(overflow.every((err) => err instanceof ThrottleQueueFullError), 'overflow rejects explicitly instead of retaining payloads');
  } finally {
    for (let i = 4; i < 4 + MAX_THROTTLE_WAITERS; i++) controllers[i].abort();
    for (let i = 0; i < 4; i++) releaseThrottleSlot();
    await Promise.allSettled(attempts);
    clearTimeout(s.refreshUsageTimer);
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
    s.refreshUsageInFlight = orig.refreshUsageInFlight; s.refreshUsageTimer = orig.refreshUsageTimer;
    s.burstDisabledUntil = orig.burstDisabledUntil;
  }
});

test('acquireThrottleSlot rejects overload before retaining a full FIFO', async () => {
  const s = require('../lib/state');
  const orig = {
    config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache,
    usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests,
    queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
    refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer,
  };
  s.config = { ...s.config, apiKey: '', overrideConcurrency: 0 };
  s.concurrencyCache = { concurrent: 0, limit: 1, softLimit: 1, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 1;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  const controllers = Array.from({ length: MAX_THROTTLE_WAITERS }, () => new AbortController());
  const queued = controllers.map((controller) => acquireThrottleSlot({}, controller.signal));
  try {
    await new Promise((resolve) => setImmediate(resolve));
    assert.strictEqual(s.throttleWaiters.length, MAX_THROTTLE_WAITERS, 'FIFO reaches its explicit bound');
    await assert.rejects(
      acquireThrottleSlot({}, new AbortController().signal),
      (err) => err instanceof ThrottleQueueFullError && err.code === 'ERR_THROTTLE_QUEUE_FULL',
    );
    assert.strictEqual(s.throttleWaiters.length, MAX_THROTTLE_WAITERS, 'overload must not retain another waiter');
  } finally {
    for (const controller of controllers) controller.abort();
    await Promise.allSettled(queued);
    clearTimeout(s.refreshUsageTimer);
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
    s.refreshUsageInFlight = orig.refreshUsageInFlight; s.refreshUsageTimer = orig.refreshUsageTimer;
  }
});

// ---- C5: notifyUpstream429 must not wake queued waiters (cascade guard) ----

test('notifyUpstream429 does not wake queued waiters (cascade guard, C5)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, throttleWaiters: s.throttleWaiters, activeRequests: s.activeRequests, queuedRequests: s.queuedRequests, usageCache: s.usageCache };
  s.config = { ...s.config, overrideConcurrency: 0, sessionCookie: '' };
  // A waiter WOULD be woken here (canStart true): limit 10, soft 5, nothing
  // active. The fix is that notifyUpstream429 skips the wake entirely.
  s.concurrencyCache = { concurrent: 0, limit: 10, softLimit: 5, boxedUntil: null, time: Date.now() };
  s.burstDisabledUntil = 0;
  s.activeRequests = 0;
  s.queuedRequests = 1;
  let resolved = false;
  s.throttleWaiters = [{ resolve: () => { resolved = true; }, reject: () => {}, signal: undefined, onAbort: null }];
  try {
    notifyUpstream429();
    assert.strictEqual(resolved, false, '429 must not wake queued waiters (cascade)');
    assert.strictEqual(s.throttleWaiters.length, 1, 'waiter still queued');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache;
    s.throttleWaiters = orig.throttleWaiters; s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
    s.usageCache = orig.usageCache;
    s.burstDisabledUntil = 0;
    try { fs.rmSync(BURST_COOLDOWN_FILE, { force: true }); } catch {}
  }
});

test('parseRetryAfter accepts delay-seconds and valid HTTP dates only', () => {
  const now = Date.UTC(2026, 6, 10, 12, 0, 0);
  assert.strictEqual(parseRetryAfter('5', now), now + 5000);
  assert.strictEqual(parseRetryAfter(' Fri, 10 Jul 2026 12:00:05 GMT ', now), now + 5000);
  assert.strictEqual(parseRetryAfter('Sunday, 06-Nov-94 08:49:37 GMT', now), Date.UTC(1994, 10, 6, 8, 49, 37));
  assert.strictEqual(parseRetryAfter('1.5', now), null);
  assert.strictEqual(parseRetryAfter('Thu, 31 Feb 2026 12:00:00 GMT', now), null);
  assert.strictEqual(parseRetryAfter('not-a-date', now), null);
});

test('Retry-After barrier blocks admission and wakes FIFO at its max expiry', () => {
  const s = require('../lib/state');
  const orig = {
    config: s.config, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched,
    activeRequests: s.activeRequests, queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
    retryAfterUntil: s.retryAfterUntil, retryAfterWakeTimer: s.retryAfterWakeTimer,
    lastBlocksToday: s.lastBlocksToday, capHealthInFlight: s.capHealthInFlight,
  };
  const originalNow = Date.now;
  const originalSetTimeout = global.setTimeout;
  const originalClearTimeout = global.clearTimeout;
  let now = Date.UTC(2026, 6, 10, 12, 0, 0);
  const timers = [];
  Date.now = () => now;
  global.setTimeout = (callback, delay) => {
    const timer = { callback, delay, cleared: false, unref() { return this; } };
    timers.push(timer);
    return timer;
  };
  global.clearTimeout = (timer) => { if (timer) timer.cleared = true; };
  s.config = { ...s.config, apiKey: '', overrideConcurrency: 0, sessionCookie: 'test-cookie' };
  s.concurrencyCache = { concurrent: 0, limit: 1, softLimit: 1, boxedUntil: null, time: now };
  s.usageEverFetched = true;
  s.activeRequests = 0;
  s.queuedRequests = 1;
  let resolved = 0;
  s.throttleWaiters = [{ resolve: () => { resolved++; }, reject: () => {}, signal: undefined, onAbort: null }];
  s.retryAfterUntil = 0;
  s.retryAfterWakeTimer = null;
  s.lastBlocksToday = 5;
  s.capHealthInFlight = Promise.resolve({ blocksToday: 5 });
  try {
    notifyUpstream429('1');
    assert.strictEqual(s.retryAfterUntil, now + 1000, 'first valid header sets an absolute deadline');
    assert.strictEqual(canStart(getEffectiveConcurrency()), false, 'admission is blocked before cap-health resolves');
    notifyUpstream429('2');
    assert.strictEqual(s.retryAfterUntil, now + 2000, 'a longer header extends the shared deadline');
    notifyUpstream429('1');
    assert.strictEqual(s.retryAfterUntil, now + 2000, 'a shorter header cannot shorten the barrier');
    assert.strictEqual(timers.filter((timer) => !timer.cleared).length, 1, 'exactly one barrier wake timer remains');
    const wakeTimer = timers.find((timer) => !timer.cleared);
    assert.strictEqual(wakeTimer.delay, 2000, 'wake is scheduled at the actual max deadline');
    now += 2000;
    wakeTimer.callback();
    assert.strictEqual(s.retryAfterUntil, 0, 'expired barrier clears its deadline');
    assert.strictEqual(resolved, 1, 'the FIFO head wakes after expiry');
    assert.strictEqual(s.activeRequests, 1, 'wake reserves the admitted slot before resolving');
  } finally {
    Date.now = originalNow;
    global.setTimeout = originalSetTimeout;
    global.clearTimeout = originalClearTimeout;
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched;
    s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
    s.retryAfterUntil = orig.retryAfterUntil; s.retryAfterWakeTimer = orig.retryAfterWakeTimer;
    s.lastBlocksToday = orig.lastBlocksToday; s.capHealthInFlight = orig.capHealthInFlight;
  }
});

// ---- P1: phantom absorption shrinks the effective limit ----

test('getEffectiveConcurrency shrinks the limit by a sustained phantom estimate (P1)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, activeRequests: s.activeRequests, phantomSamples: s.phantomSamples };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.burstDisabledUntil = 0;
  s.concurrencyCache = { concurrent: 6, limit: 8, softLimit: 4, boxedUntil: null, time: Date.now() };
  s.activeRequests = 4;
  // Three samples where observed (6) exceeds local (4) by 2 — a sustained
  // phantom. The windowed min is 2, so the limit shrinks from 8 to 6.
  s.phantomSamples = [
    { observed: 6, local: 4 },
    { observed: 6, local: 4 },
    { observed: 6, local: 4 },
  ];
  try {
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.phantomEstimate, 2, 'phantom estimate is the windowed sustained excess');
    assert.strictEqual(effective.limit, 6, 'limit shrinks by the phantom estimate');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.burstDisabledUntil = orig.burstDisabledUntil;
    s.activeRequests = orig.activeRequests; s.phantomSamples = orig.phantomSamples;
  }
});

test('phantom estimate drops a transient lag spike (windowed min, P1)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, activeRequests: s.activeRequests, phantomSamples: s.phantomSamples };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.burstDisabledUntil = 0;
  s.concurrencyCache = { concurrent: 4, limit: 8, softLimit: 4, boxedUntil: null, time: Date.now() };
  s.activeRequests = 4;
  // One sample has a lag spike (observed 9 = a just-completed request still
  // in the provider's counter), but the other two are clean (observed == local).
  // The windowed min drops the spike; estimate is 0, limit stays at 8.
  s.phantomSamples = [
    { observed: 4, local: 4 },
    { observed: 9, local: 4 },
    { observed: 4, local: 4 },
  ];
  try {
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.phantomEstimate, 0, 'transient lag spike is dropped by the windowed min');
    assert.strictEqual(effective.limit, 8, 'limit unchanged when no sustained phantom');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.burstDisabledUntil = orig.burstDisabledUntil;
    s.activeRequests = orig.activeRequests; s.phantomSamples = orig.phantomSamples;
  }
});

test('phantom absorption is skipped when the usage cache is stale (fail-safe, P1)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, burstDisabledUntil: s.burstDisabledUntil, activeRequests: s.activeRequests, phantomSamples: s.phantomSamples };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.burstDisabledUntil = 0;
  s.concurrencyCache = { concurrent: 6, limit: 8, softLimit: 4, boxedUntil: null, time: Date.now() - 60000 };
  s.activeRequests = 4;
  s.phantomSamples = [{ observed: 6, local: 4 }];
  try {
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.phantomEstimate, 0, 'stale cache → phantom estimate reported as 0');
    assert.strictEqual(effective.limit, 8, 'stale cache → limit not shrunk by outdated samples');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.burstDisabledUntil = orig.burstDisabledUntil;
    s.activeRequests = orig.activeRequests; s.phantomSamples = orig.phantomSamples;
  }
});

test('canStart blocks when held+cooling reaches the phantom-absorbed limit (P1)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, phantomSamples: s.phantomSamples, releaseCooldowns: s.releaseCooldowns };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.usageEverFetched = true;
  s.concurrencyCache = { concurrent: 4, limit: 4, softLimit: 4, boxedUntil: null, time: Date.now() };
  s.activeRequests = 2;
  s.phantomSamples = [{ observed: 4, local: 2 }]; // phantom = 2 → limit shrinks 4→2
  s.releaseCooldowns = [];
  try {
    const effective = getEffectiveConcurrency();
    assert.strictEqual(effective.limit, 2, 'limit shrunk by phantom estimate');
    assert.strictEqual(canStart(effective), false, 'held(2) >= limit(2) blocks');
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched;
    s.activeRequests = orig.activeRequests; s.phantomSamples = orig.phantomSamples; s.releaseCooldowns = orig.releaseCooldowns;
  }
});

test('canStart bursts to the hard cap even with a queue (soft clause removed)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, burstDisabledUntil: s.burstDisabledUntil, releaseCooldowns: s.releaseCooldowns, phantomSamples: s.phantomSamples, activeRequests: s.activeRequests, queuedRequests: s.queuedRequests };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.concurrencyCache = { concurrent: 0, limit: 8, softLimit: 5, boxedUntil: null, time: Date.now() };
  s.usageEverFetched = true;
  s.burstDisabledUntil = 0;
  s.releaseCooldowns = [];
  s.phantomSamples = [];
  s.queuedRequests = 2;
  try {
    s.activeRequests = 5;
    assert.strictEqual(canStart(getEffectiveConcurrency()), true); // at soft (5), with a queue, bursting to 6 is allowed (old soft clause wrongly blocked this)
    s.activeRequests = 7;
    assert.strictEqual(canStart(getEffectiveConcurrency()), true); // between soft and hard, still bursts
    s.activeRequests = 8;
    assert.strictEqual(canStart(getEffectiveConcurrency()), false); // at the hard cap, queue
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.releaseCooldowns = orig.releaseCooldowns; s.phantomSamples = orig.phantomSamples;
    s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
  }
});

// ---- P2: release cooldown blocks immediate reuse ----

test('releaseThrottleSlot pushes a cooling permit that blocks admission (P2)', () => {
  const s = require('../lib/state');
  const orig = { concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters, releaseCooldowns: s.releaseCooldowns, burstDisabledUntil: s.burstDisabledUntil, refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer, usageCache: s.usageCache, phantomSamples: s.phantomSamples };
  s.config = { ...s.config, overrideConcurrency: 0 };
  s.concurrencyCache = { concurrent: 0, limit: 2, softLimit: 2, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 1;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.releaseCooldowns = [];
  s.phantomSamples = [];
  s.burstDisabledUntil = 0;
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  try {
    releaseThrottleSlot(); // activeRequests 1→0, pushes a 2s cooldown
    assert.strictEqual(s.activeRequests, 0, 'permit released');
    assert.strictEqual(s.releaseCooldowns.length, 1, 'cooldown pushed');
    assert.strictEqual(coolingDownNow(), 1, 'one permit is cooling');
    // held(0) + cooling(1) >= limit(2)? No (1 < 2). But the next request
    // should still be admitted because there's room. Instead test the block:
    // fill the remaining slot, then the cooling permit should block.
    s.activeRequests = 1;
    const effective = getEffectiveConcurrency();
    assert.strictEqual(canStart(effective), false, 'held(1)+cooling(1) >= limit(2) blocks');
  } finally {
    s.concurrencyCache = orig.concurrencyCache; s.usageEverFetched = orig.usageEverFetched;
    s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
    s.throttleWaiters = orig.throttleWaiters; s.releaseCooldowns = orig.releaseCooldowns;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.refreshUsageInFlight = orig.refreshUsageInFlight;
    s.refreshUsageTimer = orig.refreshUsageTimer; s.usageCache = orig.usageCache;
    s.phantomSamples = orig.phantomSamples;
    if (s.cooldownWakeTimer) { clearTimeout(s.cooldownWakeTimer); s.cooldownWakeTimer = null; }
  }
});

test('releaseThrottleSlot uses state.config.releaseCooldownMs when set (config-driven cooldown)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters, releaseCooldowns: s.releaseCooldowns, burstDisabledUntil: s.burstDisabledUntil, refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer, phantomSamples: s.phantomSamples };
  s.config = { ...s.config, releaseCooldownMs: 5000 };
  s.concurrencyCache = { concurrent: 0, limit: 2, softLimit: 2, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 1;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.releaseCooldowns = [];
  s.phantomSamples = [];
  s.burstDisabledUntil = 0;
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  try {
    const before = Date.now();
    releaseThrottleSlot();
    const after = Date.now();
    assert.strictEqual(s.releaseCooldowns.length, 1, 'cooldown pushed');
    const expiry = s.releaseCooldowns[0];
    assert.ok(expiry >= before + 4500 && expiry <= after + 5500, `expiry ${expiry} not within now+5000±500 (before=${before}, after=${after})`);
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
    s.throttleWaiters = orig.throttleWaiters; s.releaseCooldowns = orig.releaseCooldowns;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.refreshUsageInFlight = orig.refreshUsageInFlight;
    s.refreshUsageTimer = orig.refreshUsageTimer; s.phantomSamples = orig.phantomSamples;
    if (s.cooldownWakeTimer) { clearTimeout(s.cooldownWakeTimer); s.cooldownWakeTimer = null; }
  }
});

test('releaseThrottleSlot falls back to RELEASE_COOLDOWN_MS when config.releaseCooldownMs is absent', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters, releaseCooldowns: s.releaseCooldowns, burstDisabledUntil: s.burstDisabledUntil, refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer, phantomSamples: s.phantomSamples };
  s.config = { ...s.config };
  delete s.config.releaseCooldownMs;
  s.concurrencyCache = { concurrent: 0, limit: 2, softLimit: 2, boxedUntil: null, time: Date.now() };
  s.usageCache = { data: { ok: true }, time: Date.now() };
  s.usageEverFetched = true;
  s.activeRequests = 1;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.releaseCooldowns = [];
  s.phantomSamples = [];
  s.burstDisabledUntil = 0;
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  try {
    const before = Date.now();
    releaseThrottleSlot();
    const after = Date.now();
    assert.strictEqual(s.releaseCooldowns.length, 1, 'cooldown pushed');
    const expiry = s.releaseCooldowns[0];
    assert.ok(expiry >= before + RELEASE_COOLDOWN_MS - 500 && expiry <= after + RELEASE_COOLDOWN_MS + 500, `expiry ${expiry} not within now+${RELEASE_COOLDOWN_MS}±500 (before=${before}, after=${after})`);
  } finally {
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests; s.queuedRequests = orig.queuedRequests;
    s.throttleWaiters = orig.throttleWaiters; s.releaseCooldowns = orig.releaseCooldowns;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.refreshUsageInFlight = orig.refreshUsageInFlight;
    s.refreshUsageTimer = orig.refreshUsageTimer; s.phantomSamples = orig.phantomSamples;
    if (s.cooldownWakeTimer) { clearTimeout(s.cooldownWakeTimer); s.cooldownWakeTimer = null; }
  }
});

test('coolingDownNow prunes expired cooldowns (P2)', () => {
  const s = require('../lib/state');
  const orig = { releaseCooldowns: s.releaseCooldowns };
  // Push an already-expired cooldown — pruning should drop it immediately.
  s.releaseCooldowns = [Date.now() - 1000];
  try {
    assert.strictEqual(coolingDownNow(), 0, 'expired cooldown is pruned');
    assert.strictEqual(s.releaseCooldowns.length, 0, 'deque emptied');
  } finally {
    s.releaseCooldowns = orig.releaseCooldowns;
  }
});

test('staggered cooldown expiries re-arm and wake each eligible FIFO waiter', () => {
  const s = require('../lib/state');
  const orig = {
    config: s.config, concurrencyCache: s.concurrencyCache, usageCache: s.usageCache,
    usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests,
    queuedRequests: s.queuedRequests, throttleWaiters: s.throttleWaiters,
    releaseCooldowns: s.releaseCooldowns, phantomSamples: s.phantomSamples,
    burstDisabledUntil: s.burstDisabledUntil, refreshUsageInFlight: s.refreshUsageInFlight,
    refreshUsageTimer: s.refreshUsageTimer, cooldownWakeTimer: s.cooldownWakeTimer,
    cooldownWakeAt: s.cooldownWakeAt,
  };
  const originalNow = Date.now;
  const originalSetTimeout = global.setTimeout;
  const originalClearTimeout = global.clearTimeout;
  let now = Date.UTC(2026, 6, 10, 12, 0, 0);
  const timers = [];
  Date.now = () => now;
  global.setTimeout = (callback, delay) => {
    const timer = {
      delay,
      cleared: false,
      fired: false,
      unref() { return this; },
      fire() { this.fired = true; callback(); },
    };
    timers.push(timer);
    return timer;
  };
  global.clearTimeout = (timer) => { if (timer) timer.cleared = true; };
  s.config = { ...s.config, apiKey: '', overrideConcurrency: 0, releaseCooldownMs: 100 };
  s.concurrencyCache = { concurrent: 0, limit: 2, softLimit: 2, boxedUntil: null, time: now };
  s.usageCache = { data: { ok: true }, time: now };
  s.usageEverFetched = true;
  s.activeRequests = 2;
  s.queuedRequests = 0;
  s.throttleWaiters = [];
  s.releaseCooldowns = [];
  s.phantomSamples = [];
  s.burstDisabledUntil = 0;
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  s.cooldownWakeTimer = null;
  s.cooldownWakeAt = 0;
  try {
    releaseThrottleSlot();
    const firstWake = s.cooldownWakeTimer;
    s.config = { ...s.config, releaseCooldownMs: 20 };
    releaseThrottleSlot();
    const earlyWake = s.cooldownWakeTimer;
    assert.deepStrictEqual(s.releaseCooldowns, [now + 20, now + 100], 'cooldowns are ordered by actual expiry');
    assert.strictEqual(firstWake.cleared, true, 'later release with earlier expiry re-arms the timer');
    assert.strictEqual(earlyWake.delay, 20, 'the re-armed timer targets the earliest expiry');
    const woken = [];
    s.queuedRequests = 2;
    s.throttleWaiters = [
      { resolve: () => { woken.push('first'); }, reject: () => {}, signal: undefined, onAbort: null },
      { resolve: () => { woken.push('second'); }, reject: () => {}, signal: undefined, onAbort: null },
    ];
    now += 20;
    earlyWake.fire();
    assert.deepStrictEqual(woken, ['first'], 'first expiry wakes only the first newly eligible waiter');
    assert.strictEqual(s.throttleWaiters.length, 1, 'second waiter remains queued behind the later cooldown');
    const lateWake = s.cooldownWakeTimer;
    assert.strictEqual(lateWake.delay, 80, 'remaining cooldown re-arms from its actual expiry');
    now += 80;
    lateWake.fire();
    assert.deepStrictEqual(woken, ['first', 'second'], 'second waiter wakes after the later cooldown expires');
    assert.strictEqual(s.throttleWaiters.length, 0, 'no eligible waiter is stranded');
  } finally {
    Date.now = originalNow;
    global.setTimeout = originalSetTimeout;
    global.clearTimeout = originalClearTimeout;
    s.config = orig.config; s.concurrencyCache = orig.concurrencyCache; s.usageCache = orig.usageCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.queuedRequests = orig.queuedRequests; s.throttleWaiters = orig.throttleWaiters;
    s.releaseCooldowns = orig.releaseCooldowns; s.phantomSamples = orig.phantomSamples;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.refreshUsageInFlight = orig.refreshUsageInFlight;
    s.refreshUsageTimer = orig.refreshUsageTimer; s.cooldownWakeTimer = orig.cooldownWakeTimer;
    s.cooldownWakeAt = orig.cooldownWakeAt;
  }
});

test('currentPhantomEstimate returns 0 with no samples (P1)', () => {
  const s = require('../lib/state');
  const orig = { phantomSamples: s.phantomSamples };
  s.phantomSamples = [];
  try {
    assert.strictEqual(currentPhantomEstimate(), 0);
  } finally {
    s.phantomSamples = orig.phantomSamples;
  }
});

test('currentPhantomEstimate takes the min over the window (P1)', () => {
  const s = require('../lib/state');
  const orig = { phantomSamples: s.phantomSamples };
  s.phantomSamples = [
    { observed: 7, local: 4 }, // excess 3
    { observed: 6, local: 4 }, // excess 2  ← min
    { observed: 8, local: 4 }, // excess 4
  ];
  try {
    assert.strictEqual(currentPhantomEstimate(), 2, 'min excess over the window');
  } finally {
    s.phantomSamples = orig.phantomSamples;
  }
});

test('doFetchUmansUsage records a phantom sample aligned to heldAtFetch (P1)', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = { config: s.config, usageCache: s.usageCache, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, burstDisabledUntil: s.burstDisabledUntil, phantomSamples: s.phantomSamples };
  global.fetch = async () => ({ ok: true, json: async () => ({ usage: { concurrent_sessions: 6 }, limits: { concurrency: { limit: 8 } } }) });
  s.config = { ...s.config, apiKey: 'test-key', upstreamBaseURL: 'https://example.test', overrideConcurrency: 0 };
  s.usageCache = { data: null, time: 0 };
  s.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  s.usageEverFetched = false;
  s.activeRequests = 4;
  s.burstDisabledUntil = 0;
  s.phantomSamples = [];
  try {
    await fetchUmansUsage({ force: true });
    assert.strictEqual(s.phantomSamples.length, 1, 'one sample recorded');
    assert.strictEqual(s.phantomSamples[0].observed, 6, 'observed is the upstream concurrent_sessions');
    assert.strictEqual(s.phantomSamples[0].local, 4, 'local is heldAtFetch captured before the await');
  } finally {
    global.fetch = origFetch;
    s.config = orig.config; s.usageCache = orig.usageCache; s.concurrencyCache = orig.concurrencyCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.phantomSamples = orig.phantomSamples;
  }
});

test('phantom sample window is bounded to PHANTOM_WINDOW entries (P1)', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = { config: s.config, usageCache: s.usageCache, concurrencyCache: s.concurrencyCache, usageEverFetched: s.usageEverFetched, activeRequests: s.activeRequests, burstDisabledUntil: s.burstDisabledUntil, phantomSamples: s.phantomSamples, refreshUsageInFlight: s.refreshUsageInFlight, refreshUsageTimer: s.refreshUsageTimer };
  let n = 0;
  global.fetch = async () => ({ ok: true, json: async () => ({ usage: { concurrent_sessions: ++n }, limits: { concurrency: { limit: 10 } } }) });
  s.config = { ...s.config, apiKey: 'test-key', upstreamBaseURL: 'https://example.test', overrideConcurrency: 0 };
  s.usageCache = { data: null, time: 0 };
  s.concurrencyCache = { concurrent: null, limit: null, softLimit: null, boxedUntil: null, time: 0 };
  s.usageEverFetched = false;
  s.activeRequests = 0;
  s.burstDisabledUntil = 0;
  s.phantomSamples = [];
  s.refreshUsageInFlight = false;
  s.refreshUsageTimer = null;
  try {
    for (let i = 0; i < PHANTOM_WINDOW + 2; i++) await fetchUmansUsage({ force: true });
    assert.strictEqual(s.phantomSamples.length, PHANTOM_WINDOW, 'window bounded');
  } finally {
    global.fetch = origFetch;
    s.config = orig.config; s.usageCache = orig.usageCache; s.concurrencyCache = orig.concurrencyCache;
    s.usageEverFetched = orig.usageEverFetched; s.activeRequests = orig.activeRequests;
    s.burstDisabledUntil = orig.burstDisabledUntil; s.phantomSamples = orig.phantomSamples;
    s.refreshUsageInFlight = orig.refreshUsageInFlight; s.refreshUsageTimer = orig.refreshUsageTimer;
  }
});

// ---- P3: extractThrottle surfaces coolingDown + phantomEstimate ----

test('extractThrottle surfaces coolingDown and phantomEstimate fields (P3)', () => {
  const orig = require('../lib/state').config;
  require('../lib/state').config = { ...orig, overrideConcurrency: 0 };
  const s = require('../lib/state');
  const origCd = s.releaseCooldowns;
  const origPs = s.phantomSamples;
  s.releaseCooldowns = [Date.now() + 1000, Date.now() + 2000];
  s.phantomSamples = [{ observed: 6, local: 4 }];
  try {
    const throttle = extractThrottle({
      limits: { concurrency: { limit: 8, hard_cap: 16, burst_pct: 1 } },
      usage: { concurrent_sessions: 6 },
    });
    assert.strictEqual(throttle.coolingDown, 2, 'two cooling permits reported');
    assert.strictEqual(throttle.phantomEstimate, 2, 'phantom estimate surfaced');
  } finally {
    require('../lib/state').config = orig;
    s.releaseCooldowns = origCd;
    s.phantomSamples = origPs;
  }
});


// ---- cap-health 429 backoff gate ----

test('decide429Backoff arms on increment', () => {
  assert.strictEqual(decide429Backoff(5, 6), true);
});

test('decide429Backoff does not arm when unchanged', () => {
  assert.strictEqual(decide429Backoff(5, 5), false);
});

test('decide429Backoff does not arm on decrement (new-day reset)', () => {
  assert.strictEqual(decide429Backoff(5, 1), false);
});

test('decide429Backoff fail-safe arms when current unknown (fetch failed)', () => {
  assert.strictEqual(decide429Backoff(5, null), true);
});

test('decide429Backoff fail-safe arms when no prior baseline (first fetch)', () => {
  assert.strictEqual(decide429Backoff(null, 3), true);
});

test('decide429Backoff arms when both unknown', () => {
  assert.strictEqual(decide429Backoff(null, null), true);
});

test('notifyUpstream429 without a cookie arms synchronously (legacy fail-safe)', () => {
  const s = require('../lib/state');
  const orig = { config: s.config, burstDisabledUntil: s.burstDisabledUntil };
  s.config = { ...s.config, sessionCookie: '' };
  s.burstDisabledUntil = 0;
  try {
    notifyUpstream429();
    assert.ok(s.burstDisabledUntil > Date.now(), 'no cookie → arm immediately');
  } finally {
    s.config = orig.config; s.burstDisabledUntil = 0;
    try { fs.rmSync(BURST_COOLDOWN_FILE, { force: true }); } catch {}
  }
});

test('notifyUpstream429 with cookie arms only when blocksToday increments', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = { config: s.config, burstDisabledUntil: s.burstDisabledUntil, lastBlocksToday: s.lastBlocksToday, capHealthCache: s.capHealthCache, capHealthInFlight: s.capHealthInFlight };
  let nextBlocks = 5;
  global.fetch = async () => ({ status: 200, json: async () => ({ blocksToday: nextBlocks }) });
  s.config = { ...s.config, sessionCookie: 'test-cookie' };
  s.burstDisabledUntil = 0;
  s.lastBlocksToday = 5;                 // baseline == current → no increment
  s.capHealthCache = { data: null, time: 0 };
  s.capHealthInFlight = null;
  try {
    notifyUpstream429();
    await new Promise(r => setImmediate(r));   // flush the fire-and-forget gate (fetchCapHealth is async → its wrapper promise resolves after the raw IIFE)
    assert.strictEqual(s.burstDisabledUntil, 0, 'no arm when blocksToday unchanged');

    nextBlocks = 6;                       // increment → real cap block
    notifyUpstream429();
    await new Promise(r => setImmediate(r));
    assert.ok(s.burstDisabledUntil > Date.now(), 'arms when blocksToday incremented');
  } finally {
    global.fetch = origFetch;
    s.config = orig.config; s.burstDisabledUntil = 0; s.lastBlocksToday = orig.lastBlocksToday;
    s.capHealthCache = orig.capHealthCache; s.capHealthInFlight = orig.capHealthInFlight;
    try { fs.rmSync(BURST_COOLDOWN_FILE, { force: true }); } catch {}
    try { fs.rmSync(CAP_HEALTH_BASELINE_FILE, { force: true }); } catch {}
  }
});

test('notifyUpstream429 with cookie arms fail-safe when cap-health fetch fails', async () => {
  const s = require('../lib/state');
  const origFetch = global.fetch;
  const orig = { config: s.config, burstDisabledUntil: s.burstDisabledUntil, lastBlocksToday: s.lastBlocksToday, capHealthCache: s.capHealthCache, capHealthInFlight: s.capHealthInFlight };
  global.fetch = async () => { throw new Error('network down'); };
  s.config = { ...s.config, sessionCookie: 'test-cookie' };
  s.burstDisabledUntil = 0;
  s.lastBlocksToday = 5;
  s.capHealthCache = { data: null, time: 0 };
  s.capHealthInFlight = null;
  try {
    notifyUpstream429();
    await new Promise(r => setImmediate(r));
    assert.ok(s.burstDisabledUntil > Date.now(), 'fetch failure → arm fail-safe');
  } finally {
    global.fetch = origFetch;
    s.config = orig.config; s.burstDisabledUntil = 0; s.lastBlocksToday = orig.lastBlocksToday;
    s.capHealthCache = orig.capHealthCache; s.capHealthInFlight = orig.capHealthInFlight;
    try { fs.rmSync(BURST_COOLDOWN_FILE, { force: true }); } catch {}
  }
});

// ---- E: config parseListenAddr IPv6 + parseDuration clamp ----

test('parseListenAddr strips IPv6 brackets (E)', () => {
  assert.deepStrictEqual(parseListenAddr('[::1]:8084'), { host: '::1', port: 8084 });
});

test('parseDuration clamps a unitless footgun to >=1s (E)', () => {
  assert.strictEqual(parseDuration('30'), 1000);   // 30ms → clamped to 1s
  assert.strictEqual(parseDuration('30s'), 30000); // explicit unit unaffected
});

// ---- E/F: requiresProxyAuth / router parity ----

test('requiresProxyAuth covers every /api and /v1 route the router serves (parity, E/F)', () => {
  // Update this list when adding a route — a mismatch is a silent auth bypass.
  const authed = [
    '/api/config', '/api/shutdown', '/api/restart', '/api/reload', '/api/clear-state',
    '/api/system/info', '/api/debug/coalesce',
    '/api/umans/sessions', '/api/umans/usage', '/api/umans/concurrency',
    '/api/umans/cooldown/clear', '/api/umans/status',
    '/v1/models/info', '/v1/models', '/v1/chat/completions', '/v1/messages',
  ];
  for (const p of authed) assert.ok(requiresProxyAuth(p), `${p} must require proxy auth`);
  for (const p of ['/', '/dashboard', '/health']) assert.strictEqual(requiresProxyAuth(p), false);
  assert.strictEqual(requiresProxyAuth('/v1/models/anything'), true);
  assert.strictEqual(requiresProxyAuth('/api/umans/anything'), true);
});

// ---- F: upstream projectStatus (pure) ----

test('projectStatus projects overall + per-model status (F)', () => {
  const out = projectStatus({
    overall: { status: 'ok', uptime_pct_24h: 99.9, latency: { ttft_ms: { p50: 120 } }, output_tokens_per_second: { p50: 80 } },
    models: { 'm1': { status: 'degraded', uptime_pct_24h: 95, latency: { ttft_ms: { p50: 300 } }, output_tokens_per_second: { p50: 40 } } },
  });
  assert.strictEqual(out.ok, true);
  assert.strictEqual(out.overall.status, 'ok');
  assert.strictEqual(out.overall.uptimePct, 99.9);
  assert.strictEqual(out.overall.ttftMsP50, 120);
  assert.strictEqual(out.overall.tpsP50, 80);
  assert.strictEqual(out.models['m1'].status, 'degraded');
  assert.strictEqual(out.models['m1'].ttftMsP50, 300);
  assert.strictEqual(out.models['m1'].tpsP50, 40);
});

test('projectStatus tolerates missing/empty fields (F)', () => {
  const out = projectStatus({});
  assert.strictEqual(out.overall, null);
  assert.deepStrictEqual(out.models, {});
});

// ---- F: finalizeSession idempotency (double-finalize guard) ----

test('finalizeSession is idempotent — a double call does not double-count (F)', () => {
  const s = state;
  const orig = {
    sessions: s.sessions, groupSummaries: s.groupSummaries, sessionsByGroup: s.sessionsByGroup,
    groupSummaryTimers: s.groupSummaryTimers, sessionsBroadcastTimer: s.sessionsBroadcastTimer,
    broadcastThrottled: s.broadcastThrottled, sessionSeq: s.sessionSeq,
  };
  s.sessions = new Map(); s.groupSummaries = new Map(); s.sessionsByGroup = new Map();
  s.groupSummaryTimers = new Map(); s.sessionsBroadcastTimer = null; s.broadcastThrottled = false;
  try {
    const sess = createSession({ model: 'm', stream: true, groupKey: 'g1' });
    sess.completionTokens = 10; sess.cachedTokens = 5; sess.promptTokens = 8; sess.firstTokenAt = Date.now() - 100;
    finalizeSession(sess, 'done');
    const turns1 = s.groupSummaries.get('g1').turnCount;
    const tokens1 = s.groupSummaries.get('g1').totalTokens;
    finalizeSession(sess, 'done'); // double — must be a no-op (endedAt guard)
    assert.strictEqual(s.groupSummaries.get('g1').turnCount, turns1, 'turnCount not double-counted');
    assert.strictEqual(s.groupSummaries.get('g1').totalTokens, tokens1, 'totalTokens not double-counted');
  } finally {
    for (const t of s.groupSummaryTimers.values()) clearTimeout(t);
    clearTimeout(s.sessionsBroadcastTimer);
    s.sessions = orig.sessions; s.groupSummaries = orig.groupSummaries; s.sessionsByGroup = orig.sessionsByGroup;
    s.groupSummaryTimers = orig.groupSummaryTimers; s.sessionsBroadcastTimer = orig.sessionsBroadcastTimer;
    s.broadcastThrottled = orig.broadcastThrottled; s.sessionSeq = orig.sessionSeq;
  }
});

// ---- coalesce investigation: why sessions don't group across turns ----

function resetCoalesceState() {
  const s = state;
  const orig = { stateMap: s.stateMap, messageHashCache: s.messageHashCache, stateMapTimers: s.stateMapTimers, sessionSeq: s.sessionSeq };
  for (const t of s.stateMapTimers.values()) clearTimeout(t);
  s.stateMap = new Map(); s.messageHashCache = new Map(); s.stateMapTimers = new Map(); s.sessionSeq = 0;
  return orig;
}
function restoreCoalesceState(orig) {
  const s = state;
  for (const t of s.stateMapTimers.values()) clearTimeout(t);
  s.stateMap = orig.stateMap; s.messageHashCache = orig.messageHashCache; s.stateMapTimers = orig.stateMapTimers; s.sessionSeq = orig.sessionSeq;
}

test('coalescing groups turn 2 into turn 1 when the replay matches exactly (baseline)', () => {
  const orig = resetCoalesceState();
  try {
    const model = 'm';
    const user1 = { role: 'user', content: 'hi' };
    const r1 = resolveGroupKey(model, [user1]);
    storeStateKey(model, [user1], 'Hello', r1.groupKey, r1.prefixChain);
    const r2 = resolveGroupKey(model, [user1, { role: 'assistant', content: 'Hello' }, { role: 'user', content: 'bye' }]);
    assert.strictEqual(r2.groupKey, r1.groupKey, 'exact replay coalesces');
  } finally { restoreCoalesceState(orig); }
});

test('coalescing survives a cache_control marker shift between turns (fix)', () => {
  // Claude Code re-marks cacheable blocks as the conversation grows: a system
  // block sent without cache_control on turn 1 gains it on turn 2. cache_control
  // is a volatile caching hint, not conversation-distinctive, so canonicalMessage
  // strips it — the prefix hash stays stable and turn 2 coalesces into turn 1.
  const orig = resetCoalesceState();
  try {
    const model = 'm';
    const system = { role: 'system', content: [{ type: 'text', text: 'You are helpful.' }] };
    const user1 = { role: 'user', content: 'hi' };
    const r1 = resolveGroupKey(model, [system, user1]);
    storeStateKey(model, [system, user1], 'Hello', r1.groupKey, r1.prefixChain);
    // Turn 2: client added cache_control to the system text block (Claude Code style).
    const systemCached = { role: 'system', content: [{ type: 'text', text: 'You are helpful.', cache_control: { type: 'ephemeral' } }] };
    const r2 = resolveGroupKey(model, [systemCached, user1, { role: 'assistant', content: 'Hello' }, { role: 'user', content: 'bye' }]);
    assert.strictEqual(r2.groupKey, r1.groupKey, 'cache_control shift must not break coalescing');
    // Vision/image blocks still distinguish conversations (cache_control is the only thing stripped).
    const r3 = resolveGroupKey(model, [{ role: 'user', content: [{ type: 'image_url', image_url: { url: 'x' } }] }]);
    assert.notStrictEqual(r3.groupKey, r1.groupKey, 'different content still forks');
  } finally { restoreCoalesceState(orig); }
});

test('coalescing survives string→array content promotion (cache_control attach)', () => {
  // Claude Code sends a user message as a plain string on turn 1, then
  // promotes it to [{type:'text',text:…,cache_control:…}] on turn 2 to cache
  // it. stableContent normalizes text-only arrays to joined text so the prefix
  // hash stays stable across the string↔array promotion.
  const orig = resetCoalesceState();
  try {
    const model = 'm';
    const user1str = { role: 'user', content: 'hi' };
    const r1 = resolveGroupKey(model, [user1str]);
    storeStateKey(model, [user1str], 'Hello', r1.groupKey, r1.prefixChain);
    // Turn 2: same user message promoted to a text-block array with cache_control.
    const user1arr = { role: 'user', content: [{ type: 'text', text: 'hi', cache_control: { type: 'ephemeral' } }] };
    const r2 = resolveGroupKey(model, [user1arr, { role: 'assistant', content: 'Hello' }, { role: 'user', content: 'bye' }]);
    assert.strictEqual(r2.groupKey, r1.groupKey, 'string→array promotion must not break coalescing');
    // A genuinely different user message still forks.
    const r3 = resolveGroupKey(model, [{ role: 'user', content: [{ type: 'text', text: 'different' }] }]);
    assert.notStrictEqual(r3.groupKey, r1.groupKey, 'different text still forks');
  } finally { restoreCoalesceState(orig); }
});

test('coalescing survives tool-use turns (extra tool_result in prefix)', () => {
  // Tool-use turns add an extra message (tool_result) between the assistant
  // response and the next user message. The stored stateChain covers
  // [lastUser, assistant] but the next prefix has [lastUser, assistant, tool_result].
  // resolveGroupKey must check multiple prefix positions to find the hit.
  const orig = resetCoalesceState();
  try {
    const model = 'm';
    const user1 = { role: 'user', content: 'what is 2+2?' };
    // Turn 1: user → assistant "Let me calculate" (text only, stores stateChain)
    const r1 = resolveGroupKey(model, [user1]);
    storeStateKey(model, [user1], 'Let me calculate', r1.groupKey, r1.prefixChain);

    // Turn 2: assistant did a tool call. The client replays:
    // [user1, assistant1, {role:'tool', content:'4'}, user2]
    // prefix = [user1, assistant1, {role:'tool', content:'4'}]
    // The tool message is EXTRA — the stored chain only covers [user1, assistant1].
    const r2 = resolveGroupKey(model, [
      user1,
      { role: 'assistant', content: 'Let me calculate' },
      { role: 'tool', content: '4' },
      { role: 'user', content: 'thanks' },
    ]);
    assert.strictEqual(r2.groupKey, r1.groupKey, 'tool-use turn must coalesce despite extra tool_result');

    // Also test Anthropic-style tool_result (user role with tool_result block)
    const r3 = resolveGroupKey(model, [
      user1,
      { role: 'assistant', content: 'Let me calculate' },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: 't1', content: '4' }] },
      { role: 'user', content: 'thanks' },
    ]);
    assert.strictEqual(r3.groupKey, r1.groupKey, 'Anthropic tool_result in user message must coalesce');
  } finally { restoreCoalesceState(orig); }
});
