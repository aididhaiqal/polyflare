//! Per-model pricing table + cost computation.
//!
//! Ported faithfully from codex-lb `app/core/usage/pricing.py` as it exists on
//! `codex-lb` main at commit `bcdc3842` (2026-07-20):
//! - `ModelPrice` (`pricing.py:13-27`)
//! - `DEFAULT_PRICING_MODELS` (`pricing.py:90-323`) — **27 entries**, copied
//!   verbatim (see the module-level note in the crate docs / task report for
//!   why this is 27 rather than the 55 the task brief expected — the current
//!   upstream table simply has 27 keys; nothing was dropped).
//! - `DEFAULT_MODEL_ALIASES` + `resolve_model_alias` (`pricing.py:325-368`)
//! - `_effective_rates` (`pricing.py:415-465`)
//! - `calculate_cost_breakdown_from_usage` (`pricing.py:479-517`)
//!
//! This module is pure: no I/O, no logging, no async.

use std::collections::HashMap;
use std::sync::LazyLock;

/// Per-1M-token pricing for a single model. Mirrors codex-lb's `ModelPrice`
/// dataclass (`pricing.py:13-27`) field-for-field.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ModelPrice {
    pub input_per_1m: f64,
    pub output_per_1m: f64,
    pub cached_input_per_1m: Option<f64>,
    pub priority_multiplier: Option<f64>,
    pub priority_input_per_1m: Option<f64>,
    pub priority_output_per_1m: Option<f64>,
    pub priority_cached_input_per_1m: Option<f64>,
    pub flex_input_per_1m: Option<f64>,
    pub flex_output_per_1m: Option<f64>,
    pub flex_cached_input_per_1m: Option<f64>,
    pub long_context_threshold_tokens: Option<f64>,
    pub long_context_input_per_1m: Option<f64>,
    pub long_context_output_per_1m: Option<f64>,
    pub long_context_cached_input_per_1m: Option<f64>,
}

/// The default per-model pricing table, ported verbatim from codex-lb's
/// `DEFAULT_PRICING_MODELS` (`pricing.py:90-323`). 27 entries — every key
/// present in the source at the time of the port.
static PRICING_MODELS: LazyLock<HashMap<&'static str, ModelPrice>> = LazyLock::new(|| {
    let mut m = HashMap::with_capacity(32);

    m.insert(
        "gpt-5.6-sol",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(0.5),
            output_per_1m: 30.0,
            priority_input_per_1m: Some(10.0),
            priority_cached_input_per_1m: Some(1.0),
            priority_output_per_1m: Some(60.0),
            flex_input_per_1m: Some(2.5),
            flex_cached_input_per_1m: Some(0.25),
            flex_output_per_1m: Some(15.0),
            long_context_threshold_tokens: Some(272_000.0),
            long_context_input_per_1m: Some(10.0),
            long_context_cached_input_per_1m: Some(1.0),
            long_context_output_per_1m: Some(45.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.6-terra",
        ModelPrice {
            input_per_1m: 2.5,
            cached_input_per_1m: Some(0.25),
            output_per_1m: 15.0,
            priority_input_per_1m: Some(5.0),
            priority_cached_input_per_1m: Some(0.5),
            priority_output_per_1m: Some(30.0),
            flex_input_per_1m: Some(1.25),
            flex_cached_input_per_1m: Some(0.125),
            flex_output_per_1m: Some(7.5),
            long_context_threshold_tokens: Some(272_000.0),
            long_context_input_per_1m: Some(5.0),
            long_context_cached_input_per_1m: Some(0.5),
            long_context_output_per_1m: Some(22.5),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.6-luna",
        ModelPrice {
            input_per_1m: 1.0,
            cached_input_per_1m: Some(0.1),
            output_per_1m: 6.0,
            priority_input_per_1m: Some(2.0),
            priority_cached_input_per_1m: Some(0.2),
            priority_output_per_1m: Some(12.0),
            flex_input_per_1m: Some(0.5),
            flex_cached_input_per_1m: Some(0.05),
            flex_output_per_1m: Some(3.0),
            long_context_threshold_tokens: Some(272_000.0),
            long_context_input_per_1m: Some(2.0),
            long_context_cached_input_per_1m: Some(0.2),
            long_context_output_per_1m: Some(9.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.5",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(0.5),
            output_per_1m: 30.0,
            flex_input_per_1m: Some(2.5),
            flex_cached_input_per_1m: Some(0.25),
            flex_output_per_1m: Some(15.0),
            priority_input_per_1m: Some(12.5),
            priority_cached_input_per_1m: Some(1.25),
            priority_output_per_1m: Some(75.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.5-pro",
        ModelPrice {
            input_per_1m: 30.0,
            output_per_1m: 180.0,
            flex_input_per_1m: Some(15.0),
            flex_output_per_1m: Some(90.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.4",
        ModelPrice {
            input_per_1m: 2.5,
            cached_input_per_1m: Some(0.25),
            output_per_1m: 15.0,
            priority_input_per_1m: Some(5.0),
            priority_cached_input_per_1m: Some(0.5),
            priority_output_per_1m: Some(30.0),
            flex_input_per_1m: Some(1.25),
            flex_cached_input_per_1m: Some(0.125),
            flex_output_per_1m: Some(7.5),
            long_context_threshold_tokens: Some(272_000.0),
            long_context_input_per_1m: Some(5.0),
            long_context_cached_input_per_1m: Some(0.5),
            long_context_output_per_1m: Some(22.5),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.4-mini",
        ModelPrice {
            input_per_1m: 0.75,
            cached_input_per_1m: Some(0.075),
            output_per_1m: 4.5,
            flex_input_per_1m: Some(0.375),
            flex_cached_input_per_1m: Some(0.0375),
            flex_output_per_1m: Some(2.25),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.4-nano",
        ModelPrice {
            input_per_1m: 0.20,
            cached_input_per_1m: Some(0.02),
            output_per_1m: 1.25,
            flex_input_per_1m: Some(0.10),
            flex_cached_input_per_1m: Some(0.01),
            flex_output_per_1m: Some(0.625),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.4-pro",
        ModelPrice {
            input_per_1m: 30.0,
            output_per_1m: 180.0,
            flex_input_per_1m: Some(15.0),
            flex_output_per_1m: Some(90.0),
            long_context_threshold_tokens: Some(272_000.0),
            long_context_input_per_1m: Some(60.0),
            long_context_output_per_1m: Some(270.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.3-codex",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            priority_input_per_1m: Some(3.5),
            priority_cached_input_per_1m: Some(0.35),
            priority_output_per_1m: Some(28.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.3",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.3-chat-latest",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.2",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            priority_multiplier: Some(2.0),
            flex_input_per_1m: Some(0.875),
            flex_cached_input_per_1m: Some(0.0875),
            flex_output_per_1m: Some(7.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.2-chat-latest",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.1",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            priority_multiplier: Some(2.0),
            flex_input_per_1m: Some(0.625),
            flex_cached_input_per_1m: Some(0.0625),
            flex_output_per_1m: Some(5.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.1-chat-latest",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            priority_multiplier: Some(2.0),
            flex_input_per_1m: Some(0.625),
            flex_cached_input_per_1m: Some(0.0625),
            flex_output_per_1m: Some(5.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5-chat-latest",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.2-codex",
        ModelPrice {
            input_per_1m: 1.75,
            cached_input_per_1m: Some(0.175),
            output_per_1m: 14.0,
            priority_input_per_1m: Some(3.5),
            priority_cached_input_per_1m: Some(0.35),
            priority_output_per_1m: Some(28.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.1-codex-max",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            priority_input_per_1m: Some(2.5),
            priority_cached_input_per_1m: Some(0.25),
            priority_output_per_1m: Some(20.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.1-codex-mini",
        ModelPrice {
            input_per_1m: 0.25,
            cached_input_per_1m: Some(0.025),
            output_per_1m: 2.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5.1-codex",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            priority_input_per_1m: Some(2.5),
            priority_cached_input_per_1m: Some(0.25),
            priority_output_per_1m: Some(20.0),
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-5-codex",
        ModelPrice {
            input_per_1m: 1.25,
            cached_input_per_1m: Some(0.125),
            output_per_1m: 10.0,
            priority_input_per_1m: Some(2.5),
            priority_cached_input_per_1m: Some(0.25),
            priority_output_per_1m: Some(20.0),
            ..ModelPrice::default()
        },
    );
    // OpenAI Images token-based pricing (per 1M tokens, USD), mirroring the
    // comment at pricing.py:290-302: text input $5.00 is used as the input
    // rate, image output $30.00 as the output rate, image-cached $2.00 as
    // the cached rate. All four image model entries currently share these
    // numbers (they mirror gpt-image-2 until OpenAI publishes per-model
    // deltas).
    m.insert(
        "gpt-image-2",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(2.0),
            output_per_1m: 30.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-image-1.5",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(2.0),
            output_per_1m: 30.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-image-1",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(2.0),
            output_per_1m: 30.0,
            ..ModelPrice::default()
        },
    );
    m.insert(
        "gpt-image-1-mini",
        ModelPrice {
            input_per_1m: 5.0,
            cached_input_per_1m: Some(2.0),
            output_per_1m: 30.0,
            ..ModelPrice::default()
        },
    );

    m
});

/// Glob-alias -> canonical-model-key table, ported verbatim from codex-lb's
/// `DEFAULT_MODEL_ALIASES` (`pricing.py:325-354`). A `Vec` (not a `HashMap`)
/// on purpose: `resolve_model_alias`'s longest-pattern-wins tie-break must
/// replicate Python's `max(..., key=...)` behavior, which keeps the
/// *first* max-length match encountered in dict-insertion order — that
/// requires preserving this exact order.
static MODEL_ALIASES: &[(&str, &str)] = &[
    ("gpt-5.6", "gpt-5.6-sol"),
    ("gpt-5.6-sol*", "gpt-5.6-sol"),
    ("gpt-5.6-terra*", "gpt-5.6-terra"),
    ("gpt-5.6-luna*", "gpt-5.6-luna"),
    ("gpt-5.5-pro*", "gpt-5.5-pro"),
    ("gpt-5.5*", "gpt-5.5"),
    ("gpt-5.4-pro*", "gpt-5.4-pro"),
    ("gpt-5.4-mini*", "gpt-5.4-mini"),
    ("gpt-5.4-nano*", "gpt-5.4-nano"),
    ("gpt-5.4*", "gpt-5.4"),
    ("gpt-5.3-codex*", "gpt-5.3-codex"),
    ("gpt-5.3-chat-latest*", "gpt-5.3-chat-latest"),
    ("gpt-5.2-codex*", "gpt-5.2-codex"),
    ("gpt-5.2-chat-latest*", "gpt-5.2-chat-latest"),
    ("gpt-5.3*", "gpt-5.3"),
    ("gpt-5.1-chat-latest*", "gpt-5.1-chat-latest"),
    ("gpt-5.2*", "gpt-5.2"),
    ("gpt-5-chat-latest*", "gpt-5-chat-latest"),
    ("gpt-5.1*", "gpt-5.1"),
    ("gpt-5*", "gpt-5"),
    ("gpt-5.1-codex-max*", "gpt-5.1-codex-max"),
    ("gpt-5.1-codex-mini*", "gpt-5.1-codex-mini"),
    ("gpt-5.1-codex*", "gpt-5.1-codex"),
    ("gpt-5-codex*", "gpt-5-codex"),
    ("gpt-image-2*", "gpt-image-2"),
    ("gpt-image-1.5*", "gpt-image-1.5"),
    ("gpt-image-1-mini*", "gpt-image-1-mini"),
    ("gpt-image-1*", "gpt-image-1"),
];

/// Minimal glob match mirroring Python's `fnmatch.fnmatchcase` for the
/// pattern shapes actually used in `MODEL_ALIASES`: every pattern is either
/// a bare literal (exact match) or a literal prefix followed by a single
/// trailing `*` wildcard. None of the current patterns use `fnmatch`'s
/// other syntax (`?`, `[seq]`), so this does not implement it — a
/// deliberate scope limitation, not an oversight (see task report).
fn glob_match(pattern: &str, text: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => text.starts_with(prefix),
        None => pattern == text,
    }
}

/// Ported from codex-lb's `resolve_model_alias` (`pricing.py:357-368`).
/// `model` must already be lowercased by the caller (mirrors
/// `get_pricing_for_model` normalizing once before calling this).
fn resolve_model_alias(normalized_model: &str) -> Option<&'static str> {
    if normalized_model.is_empty() {
        return None;
    }
    let mut best: Option<(usize, &'static str)> = None;
    for (pattern, target) in MODEL_ALIASES.iter() {
        let pattern_lower = pattern.to_lowercase();
        if glob_match(&pattern_lower, normalized_model) {
            let len = pattern.len();
            // Python's `max(matched, key=lambda item: item[0])` keeps the
            // *first* item seen on a length tie (strict `>` internally), so
            // only replace `best` on a strictly greater length.
            let replace = match best {
                Some((best_len, _)) => len > best_len,
                None => true,
            };
            if replace {
                best = Some((len, target));
            }
        }
    }
    best.map(|(_, target)| target)
}

/// Case-insensitive exact match against [`PRICING_MODELS`], falling back to
/// [`resolve_model_alias`]. Ports `get_pricing_for_model`
/// (`pricing.py:370-391`).
pub fn pricing_for_model(model: &str) -> Option<&'static ModelPrice> {
    if model.is_empty() {
        return None;
    }
    let normalized = model.to_lowercase();
    if let Some(price) = PRICING_MODELS.get(normalized.as_str()) {
        return Some(price);
    }
    let alias = resolve_model_alias(&normalized)?;
    PRICING_MODELS.get(alias)
}

/// Ports `_normalize_service_tier` (`pricing.py:408-412`): trim + lowercase,
/// empty string becomes `None`.
fn normalize_service_tier(service_tier: Option<&str>) -> Option<String> {
    let trimmed = service_tier?.trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Ports `_uses_priority_tier` (`pricing.py:394-398`).
fn uses_priority_tier(service_tier: Option<&str>) -> bool {
    matches!(
        normalize_service_tier(service_tier).as_deref(),
        Some("priority" | "fast")
    )
}

/// Ports `_uses_flex_tier` (`pricing.py:401-405`).
fn uses_flex_tier(service_tier: Option<&str>) -> bool {
    matches!(
        normalize_service_tier(service_tier).as_deref(),
        Some("flex")
    )
}

/// Ports `_effective_rates` (`pricing.py:415-465`) branch-for-branch:
/// 1. Compute `is_long_context`.
/// 2. Priority tier: use `priority_*` rates if configured, else multiply
///    base rates by `priority_multiplier` if configured, else fall through.
/// 3. Flex tier: use `flex_*` rates if configured, doubling input/cached and
///    1.5x-ing output when `is_long_context`.
/// 4. Long context (base tier only, since 2/3 already returned): swap in
///    `long_context_*` rates.
/// 5. Base rates.
fn effective_rates(
    price: &ModelPrice,
    input_tokens: f64,
    service_tier: Option<&str>,
) -> (f64, f64, f64) {
    let is_long_context = price
        .long_context_threshold_tokens
        .is_some_and(|threshold| input_tokens > threshold)
        && price.long_context_input_per_1m.is_some()
        && price.long_context_output_per_1m.is_some();

    let mut input_rate = price.input_per_1m;
    let mut cached_rate = price.cached_input_per_1m.unwrap_or(input_rate);
    let mut output_rate = price.output_per_1m;

    if uses_priority_tier(service_tier) {
        if let (Some(priority_input), Some(priority_output)) =
            (price.priority_input_per_1m, price.priority_output_per_1m)
        {
            let priority_cached = price.priority_cached_input_per_1m.unwrap_or(priority_input);
            return (priority_input, priority_cached, priority_output);
        }
        if let Some(multiplier) = price.priority_multiplier {
            input_rate *= multiplier;
            cached_rate *= multiplier;
            output_rate *= multiplier;
            return (input_rate, cached_rate, output_rate);
        }
    }

    if uses_flex_tier(service_tier) {
        if let (Some(flex_input), Some(flex_output)) =
            (price.flex_input_per_1m, price.flex_output_per_1m)
        {
            let mut flex_cached = price.flex_cached_input_per_1m.unwrap_or(flex_input);
            let mut flex_input = flex_input;
            let mut flex_output = flex_output;
            if is_long_context {
                flex_input *= 2.0;
                flex_cached *= 2.0;
                flex_output *= 1.5;
            }
            return (flex_input, flex_cached, flex_output);
        }
    }

    if is_long_context {
        // `is_long_context` guarantees both are `Some`.
        let long_context_input = price
            .long_context_input_per_1m
            .expect("is_long_context guarantees long_context_input_per_1m is Some");
        let long_context_output = price
            .long_context_output_per_1m
            .expect("is_long_context guarantees long_context_output_per_1m is Some");
        input_rate = long_context_input;
        cached_rate = price
            .long_context_cached_input_per_1m
            .unwrap_or(long_context_input);
        output_rate = long_context_output;
    }

    (input_rate, cached_rate, output_rate)
}

/// Computes total USD cost for a single request/turn's usage, mirroring
/// `calculate_cost_breakdown_from_usage`'s `total_usd` (`pricing.py:479-517`,
/// with the un-rounded `precision=None` path since callers here don't round).
///
/// `cached_input_tokens` is clamped into `[0, input_tokens]` before use,
/// exactly like `_normalize_usage` (`pricing.py:58-70`) clamps it.
pub fn cost_usd(
    price: &ModelPrice,
    input_tokens: i64,
    output_tokens: i64,
    cached_input_tokens: i64,
    service_tier: Option<&str>,
) -> f64 {
    let input = input_tokens.max(0) as f64;
    let output = output_tokens.max(0) as f64;
    let cached = (cached_input_tokens.max(0) as f64).min(input);
    let (input_rate, cached_rate, output_rate) = effective_rates(price, input, service_tier);
    let billable_input = (input - cached).max(0.0);
    (billable_input / 1_000_000.0) * input_rate
        + (cached / 1_000_000.0) * cached_rate
        + (output / 1_000_000.0) * output_rate
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_default_tier_gpt56_sol() {
        let p = pricing_for_model("gpt-5.6-sol").unwrap();
        // 100_000 input (20_000 cached), 10_000 output, default tier.
        // billable_input = 80_000 → 80_000/1e6*5.0 = 0.40; cached 20_000/1e6*0.5 = 0.01; output 10_000/1e6*30.0 = 0.30
        let c = cost_usd(p, 100_000, 10_000, 20_000, None);
        assert!((c - 0.71).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn cost_priority_tier_uses_priority_rates() {
        let p = pricing_for_model("gpt-5.6-sol").unwrap(); // priority in 10, cached 1, out 60
                                                           // 100_000 input (20_000 cached), 10_000 output → 80_000/1e6*10 + 20_000/1e6*1 + 10_000/1e6*60 = 0.80+0.02+0.60
        let c = cost_usd(p, 100_000, 10_000, 20_000, Some("priority"));
        assert!((c - 1.42).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn cost_long_context_above_threshold() {
        let p = pricing_for_model("gpt-5.6-sol").unwrap(); // long_context in 10, cached 1, out 45, threshold 272_000
                                                           // 300_000 input (0 cached), 1_000 output → 300_000/1e6*10 + 0 + 1_000/1e6*45 = 3.0 + 0.045
        let c = cost_usd(p, 300_000, 1_000, 0, None);
        assert!((c - 3.045).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn cached_clamped_to_input() {
        let p = pricing_for_model("gpt-5.6-sol").unwrap();
        // cached (999_999) clamped to input (100_000): billable 0, cached 100_000/1e6*0.5 = 0.05, out 0
        let c = cost_usd(p, 100_000, 0, 999_999, None);
        assert!((c - 0.05).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn unknown_model_has_no_price() {
        assert!(pricing_for_model("totally-made-up").is_none());
    }

    // The following are additional tests beyond the 5 required by the task
    // brief, added to pin down the trickiest branch of `_effective_rates`:
    // the flex-tier + long-context multiplier interaction, and the
    // priority-multiplier fallback (used when a model has no dedicated
    // `priority_*` rates).

    #[test]
    fn flex_tier_without_long_context_uses_flex_rates_unmultiplied() {
        // gpt-5.2: flex_input=0.875, flex_cached=0.0875, flex_output=7.0; no
        // long_context_threshold_tokens at all, so is_long_context is always
        // false regardless of input size.
        let p = pricing_for_model("gpt-5.2").unwrap();
        let c = cost_usd(p, 100_000, 5_000, 10_000, Some("flex"));
        // billable = 90_000/1e6*0.875 + 10_000/1e6*0.0875 + 5_000/1e6*7.0
        let expected = (90_000.0 / 1e6) * 0.875 + (10_000.0 / 1e6) * 0.0875 + (5_000.0 / 1e6) * 7.0;
        assert!((c - expected).abs() < 1e-9, "got {c}, expected {expected}");
        assert!((c - 0.114625).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn priority_multiplier_fallback_when_no_dedicated_priority_rates() {
        // gpt-5.2 has no priority_input_per_1m/priority_output_per_1m, only
        // priority_multiplier=2.0, so priority tier must fall back to
        // base_rate * multiplier for all three rates.
        let p = pricing_for_model("gpt-5.2").unwrap();
        let c = cost_usd(p, 100_000, 5_000, 10_000, Some("priority"));
        let expected = (90_000.0 / 1e6) * (1.75 * 2.0)
            + (10_000.0 / 1e6) * (0.175 * 2.0)
            + (5_000.0 / 1e6) * (14.0 * 2.0);
        assert!((c - expected).abs() < 1e-9, "got {c}, expected {expected}");
        assert!((c - 0.4585).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn flex_tier_with_long_context_multiplies_flex_rates() {
        // gpt-5.4-pro: flex_input=15.0, flex_output=90.0, no flex_cached
        // (falls back to flex_input); long_context_threshold_tokens=272_000.
        // Above threshold + flex tier: flex input/cached *2.0, output *1.5.
        let p = pricing_for_model("gpt-5.4-pro").unwrap();
        let c = cost_usd(p, 300_000, 1_000, 0, Some("flex"));
        let expected = (300_000.0 / 1e6) * (15.0 * 2.0) + 0.0 + (1_000.0 / 1e6) * (90.0 * 1.5);
        assert!((c - expected).abs() < 1e-9, "got {c}, expected {expected}");
        assert!((c - 9.135).abs() < 1e-9, "got {c}");
    }

    #[test]
    fn alias_resolves_to_canonical_model() {
        // "gpt-5.6" (no wildcard) is an exact alias to "gpt-5.6-sol"; verify
        // resolution actually reaches the same table entry via the alias
        // path, not just a coincidental unaliased hit.
        let direct = pricing_for_model("gpt-5.6-sol").unwrap();
        let aliased = pricing_for_model("gpt-5.6").unwrap();
        assert_eq!(direct, aliased);

        // "gpt-5.1-codex-pro-preview" matches the wildcard alias
        // "gpt-5.1-codex*" -> "gpt-5.1-codex", not the more specific
        // "gpt-5.1-codex-max*"/"gpt-5.1-codex-mini*" patterns (those require
        // a literal "-max"/"-mini" suffix this name doesn't have).
        let wildcard = pricing_for_model("gpt-5.1-codex-pro-preview").unwrap();
        assert_eq!(wildcard, pricing_for_model("gpt-5.1-codex").unwrap());
    }
}
