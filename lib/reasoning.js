'use strict';

const state = require('./state');

// Canonical reasoning-effort ordering, weakest to strongest.
// Covers OpenAI (minimal/low/medium/high), UMANS (none/low/medium/high/max),
// and common aliases clients send (xhi/xhigh -> max). Unknown values snap up
// to the nearest supported level so a "max" intent never silently downgrades.
const REASONING_RANK = { none: 0, off: 0, disabled: 0, minimal: 1, low: 2, medium: 3, high: 4, xhi: 5, xhigh: 5, max: 5 };

// Resolve a model's reasoning spec from cached /models/info. Returns null when
// the model is unknown or upstream /models/info is unavailable.
function getModelReasoning(modelId) {
  const info = state.modelInfoCache.data;
  if (!info || !modelId) return null;
  const entry = info[modelId];
  return entry?.capabilities?.reasoning || null;
}

// Map a client-supplied reasoning_effort to one the model actually supports.
// Unknown aliases (xhi, xhigh, minimal, off, disabled) normalize first, then
// snap UP to the nearest supported level so a "max" intent never downgrades.
// Models that can't disable reasoning (e.g. kimi-k2.7) drop a "none"/"off".
// Returns the resolved level, or null to leave the field untouched.
function snapReasoningLevel(modelId, requested) {
  const reasoning = getModelReasoning(modelId);
  if (!reasoning || !reasoning.supported) return null;
  const levels = (reasoning.levels || []).map((l) => String(l).toLowerCase());
  if (!requested) return null;
  const req = String(requested).toLowerCase().trim();

  if (levels.length) {
    if (levels.includes(req)) return requested;
    const reqRank = REASONING_RANK[req];
    if (reqRank == null) return null;
    const ranked = levels
      .map((l) => ({ l, r: REASONING_RANK[l] ?? -1 }))
      .filter((x) => x.r >= 0)
      .filter((x) => !(!reasoning.can_disable && x.r === 0));
    if (!ranked.length) return null;
    const above = ranked.filter((x) => x.r >= reqRank).sort((a, b) => a.r - b.r);
    const chosen = above.length ? above[0] : ranked.reduce((m, x) => (x.r > m.r ? x : m));
    return chosen.l;
  }

  if (!reasoning.can_disable && (req === 'none' || req === 'off' || req === 'disabled')) return null;
  return requested;
}

// Attach a `reasoning` object (OpenAI-style) to each model in a /v1/models
// response, derived from cached /models/info.
function enrichModelsWithReasoning(data) {
  if (!Array.isArray(data)) return data;
  return data.map((m) => {
    const reasoning = getModelReasoning(m.id);
    if (!reasoning || !reasoning.supported) return m;
    return {
      ...m,
      reasoning: {
        supported: true,
        can_disable: reasoning.can_disable,
        levels: reasoning.levels || [],
        default_level: reasoning.default_level,
      },
    };
  });
}

module.exports = {
  REASONING_RANK,
  getModelReasoning,
  snapReasoningLevel,
  enrichModelsWithReasoning,
};
