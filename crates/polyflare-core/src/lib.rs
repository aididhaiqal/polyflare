//! PolyFlare neutral core: formats, translator registry, and the trait spine.

pub mod format;
pub mod translate;

pub use format::Format;
pub use translate::{IdentityTranslator, Translator, TranslatorRegistry};
