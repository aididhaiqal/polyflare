//! Translator registry: the multi-provider spine. Same-format pairs are identity (zero cost).

use std::collections::HashMap;

use serde_json::Value;

use crate::format::Format;

/// Translates request and streaming-response JSON between two wire formats.
pub trait Translator: Send + Sync {
    fn translate_request(&self, body: Value) -> Value;
    fn translate_response_event(&self, event: Value) -> Value;
}

/// Pass-through translator used for same-format `(F, F)` pairs.
pub struct IdentityTranslator;

impl Translator for IdentityTranslator {
    fn translate_request(&self, body: Value) -> Value {
        body
    }
    fn translate_response_event(&self, event: Value) -> Value {
        event
    }
}

/// Registry keyed by `(from, to)` format. M1 registers only identity pairs.
pub struct TranslatorRegistry {
    map: HashMap<(Format, Format), Box<dyn Translator>>,
}

impl TranslatorRegistry {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    /// Registry with the M1 defaults: identity for the two native same-format pairs.
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        reg.register(
            Format::OpenAIResponses,
            Format::OpenAIResponses,
            Box::new(IdentityTranslator),
        );
        reg.register(
            Format::AnthropicMessages,
            Format::AnthropicMessages,
            Box::new(IdentityTranslator),
        );
        reg
    }

    pub fn register(&mut self, from: Format, to: Format, translator: Box<dyn Translator>) {
        self.map.insert((from, to), translator);
    }

    pub fn get(&self, from: Format, to: Format) -> Option<&dyn Translator> {
        self.map.get(&(from, to)).map(|b| b.as_ref())
    }
}

impl Default for TranslatorRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use serde_json::json;

    #[test]
    fn identity_registered_for_same_format_pairs() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg.get(Format::OpenAIResponses, Format::OpenAIResponses).is_some());
        assert!(reg.get(Format::AnthropicMessages, Format::AnthropicMessages).is_some());
    }

    #[test]
    fn cross_format_pair_absent_in_m1() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg.get(Format::AnthropicMessages, Format::OpenAIResponses).is_none());
    }

    #[test]
    fn identity_translator_passes_through() {
        let t = IdentityTranslator;
        let body = json!({"model": "gpt-5.6-sol", "input": "hi"});
        assert_eq!(t.translate_request(body.clone()), body);
        let ev = json!({"type": "response.completed"});
        assert_eq!(t.translate_response_event(ev.clone()), ev);
    }
}
