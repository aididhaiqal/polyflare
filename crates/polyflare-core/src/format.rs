//! Wire formats PolyFlare can speak on ingress and to backends.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// OpenAI Responses API (the Codex CLI's native wire format).
    OpenAIResponses,
    /// Anthropic Messages API (Claude / Claude Code).
    AnthropicMessages,
}
