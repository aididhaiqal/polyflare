//! Translator registry: the multi-provider spine. Same-format pairs are identity (zero cost).
//! `Translator` is a per-turn STATEFUL 1→N seam (SPEC-M4 §3.4): a cross-format translator (e.g.
//! Anthropic SSE → OpenAI-Responses SSE) needs per-turn state (message id, content-block index,
//! accumulated usage) and one incoming event can produce zero, one, or many outgoing events. The
//! registry stores a FACTORY per `(from, to)` pair so every turn gets its own fresh translator
//! instance — state never leaks across turns or requests.

use std::collections::HashMap;

use serde_json::Value;

use crate::format::Format;

/// Translates request and streaming-response JSON between two wire formats. Stateful: an
/// instance is scoped to a single turn (one request + its response stream), never shared across
/// turns.
pub trait Translator: Send + Sync {
    /// Rewrite an outgoing request body (e.g. model-alias remap + reasoning-effort injection —
    /// both deferred past this trait's concrete M4b translator; see SPEC-M4 §3.6/U2).
    fn translate_request(&mut self, body: Value) -> Value;
    /// Translate one incoming response-stream event into zero, one, or many outgoing events.
    fn translate_response_event(&mut self, event: Value) -> Vec<Value>;
}

/// Pass-through translator used for same-format `(F, F)` pairs. Stateless — a fresh instance per
/// turn costs nothing.
pub struct IdentityTranslator;

impl Translator for IdentityTranslator {
    fn translate_request(&mut self, body: Value) -> Value {
        body
    }
    fn translate_response_event(&mut self, event: Value) -> Vec<Value> {
        vec![event]
    }
}

/// Builds a fresh `Box<dyn Translator>` for one turn. Stored per `(from, to)` pair in the
/// registry; call `TranslatorRegistry::create` once per turn so per-turn state never leaks
/// across requests.
pub type TranslatorFactory = Box<dyn Fn() -> Box<dyn Translator> + Send + Sync>;

/// Registry keyed by `(from, to)` format, storing a translator FACTORY (not a shared instance)
/// per pair.
pub struct TranslatorRegistry {
    map: HashMap<(Format, Format), TranslatorFactory>,
}

impl TranslatorRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Registry with the M1 defaults: identity for the two native same-format pairs.
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        reg.register(
            Format::OpenAIResponses,
            Format::OpenAIResponses,
            Box::new(|| Box::new(IdentityTranslator) as Box<dyn Translator>),
        );
        reg.register(
            Format::AnthropicMessages,
            Format::AnthropicMessages,
            Box::new(|| Box::new(IdentityTranslator) as Box<dyn Translator>),
        );
        reg
    }

    pub fn register(&mut self, from: Format, to: Format, factory: TranslatorFactory) {
        self.map.insert((from, to), factory);
    }

    /// Build a fresh translator instance for this `(from, to)` pair, or `None` if unregistered.
    /// Call once per turn — the returned instance carries state for that turn only.
    pub fn create(&self, from: Format, to: Format) -> Option<Box<dyn Translator>> {
        self.map.get(&(from, to)).map(|factory| factory())
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
        assert!(reg
            .create(Format::OpenAIResponses, Format::OpenAIResponses)
            .is_some());
        assert!(reg
            .create(Format::AnthropicMessages, Format::AnthropicMessages)
            .is_some());
    }

    #[test]
    fn cross_format_pair_absent_in_m1() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg
            .create(Format::AnthropicMessages, Format::OpenAIResponses)
            .is_none());
    }

    #[test]
    fn identity_translator_passes_through() {
        let mut t = IdentityTranslator;
        let body = json!({"model": "gpt-5.6-sol", "input": "hi"});
        assert_eq!(t.translate_request(body.clone()), body);
        let ev = json!({"type": "response.completed"});
        assert_eq!(t.translate_response_event(ev.clone()), vec![ev]);
    }

    struct CountingTranslator {
        count: u32,
    }

    impl Translator for CountingTranslator {
        fn translate_request(&mut self, body: Value) -> Value {
            body
        }
        fn translate_response_event(&mut self, _event: Value) -> Vec<Value> {
            self.count += 1;
            vec![json!({"count": self.count})]
        }
    }

    #[test]
    fn factory_produces_independent_stateful_instances() {
        let mut reg = TranslatorRegistry::new();
        reg.register(
            Format::OpenAIResponses,
            Format::AnthropicMessages,
            Box::new(|| Box::new(CountingTranslator { count: 0 }) as Box<dyn Translator>),
        );

        let mut a = reg
            .create(Format::OpenAIResponses, Format::AnthropicMessages)
            .unwrap();
        assert_eq!(
            a.translate_response_event(json!({})),
            vec![json!({"count": 1})]
        );
        assert_eq!(
            a.translate_response_event(json!({})),
            vec![json!({"count": 2})]
        );

        // A second `create` for the same pair must yield a FRESH instance — `a`'s state must
        // never leak into `b`.
        let mut b = reg
            .create(Format::OpenAIResponses, Format::AnthropicMessages)
            .unwrap();
        assert_eq!(
            b.translate_response_event(json!({})),
            vec![json!({"count": 1})]
        );
    }
}
