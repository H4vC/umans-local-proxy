'use strict';

const state = require('./state');
const { fnv1a } = require('./hash');

function logCoalesce(entry) {
  state.coalesceDebug.push(entry);
  if (state.coalesceDebug.length > state.COALESCE_DEBUG_MAX) state.coalesceDebug.shift();
}

// Normalize a message to its cache-relevant identity: role + content text.
// Extra fields (refusal, reasoning_content, tool_calls, provider_specific_fields,
// …) vary between what the proxy captures and what the client replays, so
// they MUST NOT participate in the prefix hash. content null → "" so that a
// reasoning-only turn (proxy saw "" , client sends null) still coalesces.
function canonicalMessage(message) {
  const content = message?.content;
  return {
    role: message?.role || '',
    content: typeof content === 'string' ? content : (content == null ? '' : JSON.stringify(content)),
  };
}

// Cached per-message hash. JSON.stringify is the expensive part; cache it so
// a system prompt reused across 50 turns is stringified+hashed exactly once.
// True LRU: on a hit, delete + re-insert to move to end (most-recently-used).
// Map iteration order is insertion order, so keys().next() returns the LRU.
function messageHash(message) {
  const json = JSON.stringify(canonicalMessage(message));
  let h = state.messageHashCache.get(json);
  if (h !== undefined) {
    state.messageHashCache.delete(json);
    state.messageHashCache.set(json, h);
    return h;
  }
  h = fnv1a(json);
  state.messageHashCache.set(json, h);
  if (state.messageHashCache.size > 5000) {
    const lru = state.messageHashCache.keys().next().value;
    state.messageHashCache.delete(lru);
  }
  return h;
}

// Rolling chain hash over [model, ...messages]. Each step is H(prev, msgHash)
// — O(1) per message, O(n) per request (n = message count, not token count).
function chainHash(model, messages, fromChain) {
  let chain = fromChain || fnv1a(model || '');
  for (let i = 0; i < messages.length; i++) {
    chain = fnv1a(chain + messageHash(messages[i]));
  }
  return chain;
}

// Resolve the logical session for an incoming request. Called at arrival,
// before createSession. Returns { groupKey, prefixChain } so storeStateKey
// can extend the chain without re-walking the prefix.
function resolveGroupKey(model, messages) {
  const prefix = messages.slice(0, -1);
  const prefixChain = chainHash(model, prefix);
  const hit = state.stateMap.get(prefixChain);
  const groupKey = hit || newSessionId();
  logCoalesce({ t: Date.now(), type: 'resolve', model, msgCount: messages.length, prefixMsgCount: prefix.length, prefixChain, stateMapHit: !!hit, groupKey, stateMapSize: state.stateMap.size });
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
  logCoalesce({ t: Date.now(), type: 'store', model, msgCount: messages.length, stateChain, prefixChain, groupKey, responseContentLen: (responseContent || '').length, responseContentPreview: (responseContent || '').slice(0, 100), lastUserMsg: JSON.stringify(lastUser).slice(0, 200), lastUserHash: messageHash(lastUser), responseMsgHash: messageHash(responseMsg) });
  setTimeout(() => { state.stateMap.delete(stateChain); }, state.COALESCE_TTL_MS).unref();
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
