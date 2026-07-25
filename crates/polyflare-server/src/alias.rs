//! The resolved target for a persisted cross-protocol translation route.

use polyflare_core::Provider;

/// Where an aliased client model routes, and under what target model + reasoning effort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAlias {
    pub target: TranslationTarget,
    pub target_model: String,
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranslationTarget {
    Builtin(Provider),
    Custom(String),
}
