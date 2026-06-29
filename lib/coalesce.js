'use strict';

const state = require('./state');
const { fnv1a32, fnv1aMixNum } = require('./hash');
// Defensive: stateMapTimers was added to state.js after the running process
// started. state.js is never purged on hot reload, so if the field is missing
// (old state singleton), initialize it here.
if (!(state.stateMapTimers instanceof Map)) state.stateMapTimers = new Map();

function logCoalesce(entry) {
  state.coalesceDebug.push(entry);
  if (state.coalesceDebug.length > state.COALESCE_DEBUG_MAX) state.coalesceDebug.shift();
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
// pass through; array content (system prompts, vision) is stringified with
// `cache_control` stripped — it's a volatile caching hint that clients (Claude
// Code) shift between turns (re-marking cacheable blocks as the conversation
// grows). Leaving it in the hash changes the prefix hash every turn, so each
// turn forks a new group and sessions never coalesce. Vision/image blocks and
// all other fields are kept, so distinct conversations still distinguish.
function stableContent(content) {
  if (typeof content === 'string') return content;
  if (content == null) return '';
  if (Array.isArray(content)) return JSON.stringify(content.map(withoutCacheControl));
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
// concatenation. Returns a hex string for use as a cache key.
function chainHash(model, messages, fromChain) {
  // fromChain is a hex string from a prior chainHash() call; parse it back
  // to uint32 so fnv1aMixNum operates on a proper numeric seed.
  let chain = (fromChain != null ? parseInt(fromChain, 16) : fnv1a32(model || ''));
  for (let i = 0; i < messages.length; i++) {
    chain = fnv1aMixNum(chain, messageHash(messages[i]));
  }
  return chain.toString(16).padStart(8, '0');
}

// Resolve the logical session for an incoming request. Called at arrival,
// before createSession. Returns { groupKey, prefixChain } so storeStateKey
// can extend the chain without re-walking the prefix.
function resolveGroupKey(model, messages) {
  const prefix = messages.slice(0, -1);
  const prefixChain = chainHash(model, prefix);
  const hit = state.stateMap.get(prefixChain);
  const groupKey = hit || newSessionId();
  logCoalesce({ t: Date.now(), type: 'resolve', msgCount: messages.length, prefixChain, stateMapHit: !!hit, groupKey, stateMapSize: state.stateMap.size });
  return { groupKey, prefixChain };
}

// Store the conversation state after a request completes, so the NEXT request
// whose prefix matches this state hits the stateMap and coalesces. Called at
// session completion — never in the streaming hot path. Accepts the saved
// prefixChain from resolveGroupKey to avoid re-walking the prefix.
function storeStateKey(model, messages, responseContent, groupKey, prefixChain) {
  const lastUser = messages[messages.length - 1];
  const responseMsg = { role: 'assistant', content: responseContent || '' };
  const stateChain = chainHash(model, [lastUser, responseMsg], prefixChain);
  state.stateMap.set(stateChain, groupKey);
  logCoalesce({ t: Date.now(), type: 'store', msgCount: messages.length, stateChain, prefixChain, groupKey, responseContentLen: (responseContent || '').length });
  const prevTimer = state.stateMapTimers.get(stateChain);
  if (prevTimer) clearTimeout(prevTimer);
  const timer = setTimeout(() => { state.stateMap.delete(stateChain); state.stateMapTimers.delete(stateChain); }, state.COALESCE_TTL_MS);
  timer.unref();
  state.stateMapTimers.set(stateChain, timer);
}

function newSessionId() {
  state.sessionSeq = (state.sessionSeq + 1) % 0x100000000;
  return Date.now().toString(36) + state.sessionSeq.toString(36).padStart(8, '0');
}

module.exports = {
  logCoalesce,
  canonicalMessage,
  messageHash,
  chainHash,
  resolveGroupKey,
  storeStateKey,
  newSessionId,
};
