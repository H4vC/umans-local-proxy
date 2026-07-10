'use strict';

const state = require('./state');
const { fnv1a32, fnv1aMixNum } = require('./hash');
// Defensive: stateMapTimers was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!(state.stateMapTimers instanceof Map)) state.stateMapTimers = new Map();

const MAX_STATE_MAP_ENTRIES = 5000;
const MAX_COALESCE_DEBUG_ENTRIES = 200;

function coalesceDebugLimit() {
  const configured = Number(state.COALESCE_DEBUG_MAX);
  return Number.isFinite(configured) && configured >= 0
    ? Math.min(configured, MAX_COALESCE_DEBUG_ENTRIES)
    : MAX_COALESCE_DEBUG_ENTRIES;
}

function evictOldestStateKey() {
  const key = state.stateMap.keys().next().value;
  if (key == null) return;
  state.stateMap.delete(key);
  clearTimeout(state.stateMapTimers.get(key));
  state.stateMapTimers.delete(key);
}

function logCoalesce(entry) {
  state.coalesceDebug.push(entry);
  const limit = coalesceDebugLimit();
  if (state.coalesceDebug.length > limit) state.coalesceDebug.splice(0, state.coalesceDebug.length - limit);
}

// Normalize a message to its cache-relevant identity: role + content text.
// Extra fields (refusal, reasoning_content, tool_calls, provider_specific_fields,
// …) vary between what the proxy captures and what the client replays, so
// they MUST NOT participate in the prefix hash. content null → "" so that a
// reasoning-only turn (proxy saw "" , client sends null) still coalesces.
//
// Assistant asymmetry: the proxy captures only the visible TEXT of an assistant
// turn (responseContent is a string; thinking/tool_use blocks are never seen
// structurally). Clients on the Anthropic path replay the assistant turn as an
// array of content blocks — [{type:'thinking',…,signature}, {type:'text',text},
// {type:'tool_use',…}] — whenever the turn had thinking or tool-use. Stringifying
// that array (the old behavior) could never equal the proxy's bare text string,
// so every such turn forked a new group forever after. For assistant messages we
// therefore flatten array content to its joined text (dropping thinking/tool_use
// blocks; their effect on the next turn is captured by the subsequent tool/user
// messages in the prefix). User/system/tool roles keep full JSON.stringify so
// vision and other non-text user content still distinguishes conversations.
function assistantText(content) {
  if (typeof content === 'string') return content;
  if (content == null) return '';
  if (Array.isArray(content)) {
    let out = '';
    for (let i = 0; i < content.length; i++) {
      const part = content[i];
      if (part && typeof part.text === 'string') out += part.text;
    }
    return out;
  }
  return JSON.stringify(content);
}

function canonicalMessage(message) {
  const role = message?.role || '';
  const content = message?.content;
  return {
    role,
    content: role === 'assistant'
      ? assistantText(content)
      : stableContent(content),
  };
}

// Non-assistant content normalized to its conversation-distinctive form. Strings
// pass through; array content has `cache_control` stripped (a volatile caching
// hint that clients like Claude Code shift between turns). Text-only arrays are
// further reduced to their joined text: clients promote a plain string to
// [{type:'text',text:…,cache_control:…}] when they first cache that message, and
// without this normalization the hash changes on promotion and every subsequent
// turn forks a new group. Arrays with any non-text block (vision, tool_result)
// keep full stringify so distinct content still distinguishes.
function stableContent(content) {
  if (typeof content === 'string') return content;
  if (content == null) return '';
  if (Array.isArray(content)) {
    let text = '';
    for (let i = 0; i < content.length; i++) {
      const b = content[i];
      if (b && b.type === 'text' && typeof b.text === 'string') text += b.text;
      else return JSON.stringify(content.map(withoutCacheControl));
    }
    return text;
  }
  return JSON.stringify(content);
}

function withoutCacheControl(block) {
  if (!block || typeof block !== 'object' || !('cache_control' in block)) return block;
  const copy = { ...block };
  delete copy.cache_control;
  return copy;
}

const MAX_MESSAGE_HASH_CACHE_ENTRIES = 5000;
const MAX_MESSAGE_HASH_CACHE_KEY_CHARS = 8192;

// Cached per-message hash. JSON.stringify is the expensive part; cache it so
// a system prompt reused across 50 turns is stringified+hashed exactly once.
// True LRU: on a hit, delete + re-insert to move to end (most-recently-used).
// Large prompts are hashed but not cached: retaining full serialized prompt
// strings as Map keys turns this CPU cache into a memory sink.
function messageHash(message) {
  const json = JSON.stringify(canonicalMessage(message));
  const cacheable = json.length <= MAX_MESSAGE_HASH_CACHE_KEY_CHARS;
  if (cacheable) {
    let h = state.messageHashCache.get(json);
    if (typeof h === 'number') {
      state.messageHashCache.delete(json);
      state.messageHashCache.set(json, h);
      return h;
    }
  }
  const h = fnv1a32(json);
  if (!cacheable) return h;
  state.messageHashCache.set(json, h);
  if (state.messageHashCache.size > MAX_MESSAGE_HASH_CACHE_ENTRIES) {
    const lru = state.messageHashCache.keys().next().value;
    state.messageHashCache.delete(lru);
  }
  return h;
}

// Rolling chain hash over [model, ...messages]. Each step mixes the prior
// uint32 chain with the next message's uint32 hash — O(1) per message,
// O(n) per request (n = message count, not token count). Numeric mixing
// gives full 256-value-per-byte entropy vs the 16 values of hex-string
// concatenation.
// chainHashNum returns the raw uint32; chainHash wraps it as the hex string
// used for stateMap keys and the public fromChain API (a hex string from a
// prior chainHash() call).
function chainHashNum(model, messages, fromChainNum) {
  let chain = (fromChainNum != null ? fromChainNum : fnv1a32(model || ''));
  for (let i = 0; i < messages.length; i++) {
    chain = fnv1aMixNum(chain, messageHash(messages[i]));
  }
  return chain >>> 0;
}

function chainHash(model, messages, fromChain) {
  return chainHashNum(model, messages, fromChain != null ? parseInt(fromChain, 16) >>> 0 : null)
    .toString(16).padStart(8, '0');
}

// Resolve the logical session for an incoming request. Called at arrival,
// before createSession. Returns { groupKey, prefixChain } so storeStateKey
// can extend the chain without re-walking the prefix.
//
// Tool-use coalescing: storeStateKey stores the chain at position
// [lastUser, assistant_response] — i.e. after the assistant turn. The next
// turn's prefix may have EXTRA messages after the assistant (tool_result for
// OpenAI, user/tool_result for Anthropic) before the new user message. The
// full-prefix chain will never match the stored chain (different length).
// Fix: walk the prefix incrementally, saving every intermediate chain, then
// check the stateMap from the longest prefix backwards. The first hit is the
// most recent stored state — the tool_result messages between the hit
// position and the prefix end are the gap. prefixChain is always the FULL
// prefix so storeStateKey extends from the right position.
function resolveGroupKey(model, messages) {
  const prefix = messages.slice(0, -1);
  // Incremental chain: chains[i] = hash of prefix[0..i-1]. O(n) total.
  let chain = fnv1a32(model || '');
  const chains = new Array(prefix.length + 1);
  chains[0] = chain.toString(16).padStart(8, '0');
  for (let i = 0; i < prefix.length; i++) {
    chain = fnv1aMixNum(chain, messageHash(prefix[i]));
    chains[i + 1] = chain.toString(16).padStart(8, '0');
  }
  const prefixChain = chains[prefix.length];
  const prefixChainNum = chain >>> 0;
  // Check from the longest prefix (most specific) backwards. The stored
  // stateChain sits at the position after [lastUser, assistant] — if the
  // prefix has trailing tool_result messages, the hit is at a shorter chain.
  let hit = null;
  for (let i = chains.length - 1; i >= 0; i--) {
    hit = state.stateMap.get(chains[i]);
    if (hit) break;
  }
  const groupKey = hit || newSessionId();
  logCoalesce({ t: Date.now(), type: 'resolve', msgCount: messages.length, prefixChain, stateMapHit: !!hit, groupKey, stateMapSize: state.stateMap.size });
  return { groupKey, prefixChain, prefixChainNum };
}

// Store the conversation state after a request completes, so the NEXT request
// whose prefix matches this state hits the stateMap and coalesces. Called at
// session completion — never in the streaming hot path. Accepts the saved
// prefixChain from resolveGroupKey to avoid re-walking the prefix.
function storeStateKey(model, messages, responseContent, groupKey, prefixChain, prefixChainNum) {
  const lastUser = messages[messages.length - 1];
  const responseMsg = { role: 'assistant', content: responseContent || '' };
  // Extend the prefix chain numerically when the caller (chat.js) passes the
  // uint32 from resolveGroupKey — avoids the uint32->hex->parseInt round-trip
  // the public chainHash API requires. Tests call without it; fall back to
  // parsing the hex prefixChain so the 5-arg signature still works.
  const seed = typeof prefixChainNum === 'number'
    ? prefixChainNum
    : (prefixChain != null ? parseInt(prefixChain, 16) >>> 0 : null);
  const stateChain = chainHashNum(model, [lastUser, responseMsg], seed).toString(16).padStart(8, '0');
  if (!state.stateMap.has(stateChain) && state.stateMap.size >= MAX_STATE_MAP_ENTRIES) evictOldestStateKey();
  state.stateMap.set(stateChain, groupKey);
  logCoalesce({ t: Date.now(), type: 'store', msgCount: messages.length, stateChain, prefixChain, groupKey, responseContentLen: (responseContent || '').length });
  clearTimeout(state.stateMapTimers.get(stateChain));
  const timer = setTimeout(() => { state.stateMap.delete(stateChain); state.stateMapTimers.delete(stateChain); }, state.COALESCE_TTL_MS);
  timer.unref();
  state.stateMapTimers.set(stateChain, timer);
}

function newSessionId() {
  state.sessionSeq = (state.sessionSeq + 1) % 0x100000000;
  return Date.now().toString(36) + state.sessionSeq.toString(36).padStart(8, '0');
}

while (state.stateMap.size > MAX_STATE_MAP_ENTRIES) evictOldestStateKey();
for (const [key, timer] of state.stateMapTimers) {
  if (!state.stateMap.has(key)) {
    clearTimeout(timer);
    state.stateMapTimers.delete(key);
  }
}
while (state.messageHashCache.size > MAX_MESSAGE_HASH_CACHE_ENTRIES) state.messageHashCache.delete(state.messageHashCache.keys().next().value);
const initialDebugLimit = coalesceDebugLimit();
if (state.coalesceDebug.length > initialDebugLimit) state.coalesceDebug.splice(0, state.coalesceDebug.length - initialDebugLimit);


module.exports = {
  logCoalesce,
  canonicalMessage,
  messageHash,
  chainHash,
  resolveGroupKey,
  storeStateKey,
  newSessionId,
  MAX_STATE_MAP_ENTRIES,
  MAX_COALESCE_DEBUG_ENTRIES,
};
