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

/// A Claude request-translation alias: a stable public id, the `needle` [`lookup_alias`] matches
/// on (Claude Code sends dated ids such as `claude-opus-4-1-20250805`), and its target. These rows
/// are routing definitions for `/v1/messages`, not model-discovery entries; `crate::catalog`
/// deliberately keeps them out of Codex and generic OpenAI pickers.
pub struct SyntheticModel {
    /// Stable client-facing family id used for collision reservation and operator diagnostics.
    pub id: &'static str,
    /// Case-insensitive substring `lookup_alias` matches an inbound `model` string against.
    pub needle: &'static str,
    /// Human-readable name for the catalog.
    pub display_name: &'static str,
    /// The routing target.
    pub alias: ModelAlias,
}

/// The translation model definitions (first match wins in [`lookup_alias`]). U2: confirm exact
/// strings/pairs. Defaults per SPEC-M4 §3.6/§7: `opus` -> Codex `gpt-5.6-sol` @ high, `sonnet` ->
/// `gpt-5.6-terra` @ medium, `haiku` -> `gpt-5.6-luna` @ low. Edit here to add/repoint synthetic
/// models without touching lookup or catalog logic.
pub fn synthetic_models() -> Vec<SyntheticModel> {
    vec![
        SyntheticModel {
            id: "claude-opus-4-1",
            needle: "opus",
            display_name: "Claude Opus 4.1 (via Codex gpt-5.6-sol)",
            alias: ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-sol".to_string(),
                reasoning_effort: Some("high".to_string()),
            },
        },
        SyntheticModel {
            id: "claude-sonnet-4-5",
            needle: "sonnet",
            display_name: "Claude Sonnet 4.5 (via Codex gpt-5.6-terra)",
            alias: ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-terra".to_string(),
                reasoning_effort: Some("medium".to_string()),
            },
        },
        SyntheticModel {
            id: "claude-haiku-4-5",
            needle: "haiku",
            display_name: "Claude Haiku 4.5 (via Codex gpt-5.6-luna)",
            alias: ModelAlias {
                target_provider: Provider::Codex,
                target_model: "gpt-5.6-luna".to_string(),
                reasoning_effort: Some("low".to_string()),
            },
        },
    ]
}

/// Look up the alias for a client-supplied `model` string: a case-insensitive substring match
/// against [`synthetic_models`]'s needles, in order. `None` means no alias entry: the caller must
/// take the native (same-provider) ingress path.
pub fn lookup_alias(model: &str) -> Option<ModelAlias> {
    let lower = model.to_lowercase();
    synthetic_models()
        .into_iter()
        .find(|m| lower.contains(m.needle))
        .map(|m| m.alias)
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
