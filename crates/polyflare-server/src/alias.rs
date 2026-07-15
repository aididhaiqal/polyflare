//! M4b-wiring: the model-alias config that lets a Claude `/v1/messages` client reach the Codex
//! pool as a mapped model (SPEC-M4 §3.6). An alias maps a client-supplied `model` string to a
//! target provider + model + reasoning-effort override; a request whose model has no entry here
//! takes the native (same-provider) ingress path unchanged.

use polyflare_core::Provider;

/// Where an aliased client model routes, and under what target model + reasoning effort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAlias {
    pub target_provider: Provider,
    pub target_model: String,
    pub reasoning_effort: Option<String>,
}

/// U2: confirm exact model strings + pairs. Proposed defaults per SPEC-M4 §3.6/§7: a client
/// model containing `opus` -> Codex `gpt-5.6-sol` @ high effort; `sonnet` -> Codex
/// `gpt-5.6-terra` @ medium; `haiku` -> Codex `gpt-5.6-luna` @ low. Kept as a single
/// easy-to-edit function (an ordered list of needle/alias pairs, first match wins) so the exact
/// strings/pairs can be swapped without touching the lookup logic in `lookup_alias`.
fn default_aliases() -> Vec<(&'static str, ModelAlias)> {
    vec![
        (
            "opus",
            ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-sol".to_string(),
                reasoning_effort: Some("high".to_string()),
            },
        ),
        (
            "sonnet",
            ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-terra".to_string(),
                reasoning_effort: Some("medium".to_string()),
            },
        ),
        (
            "haiku",
            ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-luna".to_string(),
                reasoning_effort: Some("low".to_string()),
            },
        ),
    ]
}

/// Look up the alias for a client-supplied `model` string: a case-insensitive substring match
/// against `default_aliases`'s needles, in order (Claude Code sends full IDs like
/// `claude-opus-4-1-20250805`, not the bare tier name, so substring — not exact — match is
/// required). `None` means no alias entry: the caller must take the native (same-provider)
/// ingress path.
pub fn lookup_alias(model: &str) -> Option<ModelAlias> {
    let lower = model.to_lowercase();
    default_aliases()
        .into_iter()
        .find(|(needle, _)| lower.contains(needle))
        .map(|(_, alias)| alias)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opus_model_string_maps_to_codex_sol_high() {
        let alias = lookup_alias("claude-opus-4-1-20250805").expect("opus should alias");
        assert_eq!(alias.target_provider, Provider::Codex);
        assert_eq!(alias.target_model, "gpt-5.6-sol");
        assert_eq!(alias.reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn sonnet_model_string_maps_to_codex_terra_medium() {
        let alias = lookup_alias("claude-sonnet-4-5-20250929").expect("sonnet should alias");
        assert_eq!(alias.target_provider, Provider::Codex);
        assert_eq!(alias.target_model, "gpt-5.6-terra");
        assert_eq!(alias.reasoning_effort.as_deref(), Some("medium"));
    }

    #[test]
    fn haiku_model_string_maps_to_codex_luna_low() {
        let alias = lookup_alias("claude-haiku-4-5-20251001").expect("haiku should alias");
        assert_eq!(alias.target_provider, Provider::Codex);
        assert_eq!(alias.target_model, "gpt-5.6-luna");
        assert_eq!(alias.reasoning_effort.as_deref(), Some("low"));
    }

    #[test]
    fn match_is_case_insensitive() {
        let alias = lookup_alias("CLAUDE-OPUS-4-1").expect("case-insensitive match");
        assert_eq!(alias.target_model, "gpt-5.6-sol");
    }

    #[test]
    fn unknown_model_has_no_alias() {
        assert!(lookup_alias("some-other-model").is_none());
        assert!(lookup_alias("gpt-5.6-sol").is_none());
    }
}
