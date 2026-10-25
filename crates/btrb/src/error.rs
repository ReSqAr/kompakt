//! The one error type of the runtime's seam.
//!
//! An app has its own error type; it crosses the seam as an [`Error`] carrying
//! the app's error as its source, so the runtime can print a chain without
//! knowing any app's types.

use crate::config::ConfigError;

#[derive(Debug)]
pub struct Error {
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    pub fn new(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self {
            source: source.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Self::new(e)
    }
}

impl From<nvzn::Error> for Error {
    fn from(e: nvzn::Error) -> Self {
        Self::new(e)
    }
}
