//! The backend provider an account belongs to — decides which executor + backend wire `Format`
//! services a request. Carries no secret data, so (unlike `Account`/`PreparedRequest`) it needs no
//! redacting `Debug`.

use std::fmt;
use std::str::FromStr;

/// Which backend pool an account belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    /// The Codex (OpenAI-Responses) backend pool.
    Codex,
    /// The Anthropic (Messages) backend pool.
    Anthropic,
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Provider::Codex => "codex",
            Provider::Anthropic => "anthropic",
        })
    }
}

/// Returned when a stored `provider` column value doesn't match a known provider.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown provider: {0}")]
pub struct UnknownProvider(pub String);

impl FromStr for Provider {
    type Err = UnknownProvider;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "codex" => Ok(Provider::Codex),
            "anthropic" => Ok(Provider::Anthropic),
            other => Err(UnknownProvider(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_display_and_from_str() {
        assert_eq!(Provider::Codex.to_string(), "codex");
        assert_eq!(Provider::Anthropic.to_string(), "anthropic");
        assert_eq!("codex".parse::<Provider>().unwrap(), Provider::Codex);
        assert_eq!(
            "anthropic".parse::<Provider>().unwrap(),
            Provider::Anthropic
        );
    }

    #[test]
    fn unknown_provider_string_is_rejected() {
        let err = "bogus".parse::<Provider>().unwrap_err();
        assert_eq!(err.0, "bogus");
    }
}
