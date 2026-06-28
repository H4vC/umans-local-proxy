'use strict';

const state = require('./state');

// Canonical reasoning-effort ordering, weakest to strongest.
// Covers OpenAI (minimal/low/medium/high), UMANS (none/low/medium/high/max),
// and common aliases clients send (xhi/xhigh -> max). These known names only
// ANCHOR the ranking: any level UMANS declares under an unknown name is
// interpolated by its declared position (rankDeclaredLevels), so a future
// rename of the top level never silently downgrades a max request.
const REASONING_RANK = { none: 0, off: 0, disabled: 0, minimal: 1, low: 2, medium: 3, high: 4, xhi: 5, xhigh: 5, max: 5 };

// Resolve a model's reasoning spec from cached /models/info. Returns null when
// the model is unknown or upstream /models/info is unavailable.
function getModelReasoning(modelId) {
  const info = state.modelInfoCache.data;
  if (!info || !modelId) return null;
  const entry = info[modelId];
  return entry?.capabilities?.reasoning || null;
}

// Rank a model's declared reasoning levels monotonically by DECLARED ORDER,
// not by hardcoded name. Known names anchor to REASONING_RANK; unknown names
// interpolate between surrounding known anchors (and extrapolate at the ends).
// Because UMANS declares levels in ascending order, the last declared level
// always carries the highest rank — so a max intent (xhi/xhigh/max) snaps to
// the highest declared level even when its name is not in REASONING_RANK.
// Returns [{ l, r }] over the input order (off-levels included at rank 0).
function rankDeclaredLevels(levels) {
  const anchors = levels
    .map((l, i) => ({ i, r: REASONING_RANK[l] }))
    .filter((x) => x.r != null);
  return levels.map((l, i) => {
    const known = REASONING_RANK[l];
    if (known != null) return { l, r: known };
    const prev = [...anchors].reverse().find((a) => a.i < i);
    const next = anchors.find((a) => a.i > i);
    let r;
    if (prev && next) r = prev.r + ((next.r - prev.r) * (i - prev.i)) / (next.i - prev.i);
    else if (prev) r = prev.r + (i - prev.i);
    else if (next) r = next.r - (next.i - i);
    else r = i + 1;
    return { l, r };
  });
}

// Map a client-supplied reasoning_effort to one the model actually supports.
// Unknown aliases (xhi, xhigh, minimal, off, disabled) normalize first, then
// snap UP to the nearest supported level so a "max" intent never downgrades.
// Declared levels are ranked by position (rankDeclaredLevels), so a renamed
// or newly added top level still receives max intents. Models that can't
// disable reasoning (e.g. kimi-k2.7) drop a "none"/"off".
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
    const ranked = rankDeclaredLevels(levels).filter((x) => !(!reasoning.can_disable && x.r === 0));
    if (!ranked.length) return null;
    // A max intent (xhi/xhigh/max) always lands on the highest declared
    // level — the last by position — even when its name is unknown to
    // REASONING_RANK, so a future rename of UMANS's top level never
    // silently downgrades a max request.
    if (reqRank >= 5) return ranked.reduce((m, x) => (x.r >= m.r ? x : m)).l;
    const above = ranked.filter((x) => x.r >= reqRank).sort((a, b) => a.r - b.r);
    const chosen = above.length ? above[0] : ranked.reduce((m, x) => (x.r > m.r ? x : m));
    return chosen.l;
  }

  if (!reasoning.can_disable && (req === 'none' || req === 'off' || req === 'disabled')) return null;
  return requested;
}

// Enrich each model in a /v1/models response with (1) an OpenAI-style
// `reasoning` object derived from cached /models/info, and (2) a
// `supported_endpoint_types: ["openai"]` advertisement.
//
// (2) steers OMP `discovery.type: proxy`: OMP derives each discovered model's
// `api` from this field (anthropic -> anthropic-messages, openai ->
// openai-completions, else dropped/fallback). Advertising openai-only keeps
// discovery on the OpenAI path where UMANS emits a clean `reasoning_content`
// field, instead of the Anthropic path that surfaces thinking markers in
// content for reasoning models. The proxy still serves /v1/messages as a
// passthrough for direct Anthropic clients; this affects discovery only.
function enrichModelsWithReasoning(data) {
  if (!Array.isArray(data)) return data;
  return data.map((m) => {
    const reasoning = getModelReasoning(m.id);
    const stamped = { ...m, supported_endpoint_types: ['openai'] };
    if (!reasoning || !reasoning.supported) return stamped;
    return {
      ...stamped,
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
  rankDeclaredLevels,
  enrichModelsWithReasoning,
};
