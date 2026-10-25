//! The value grammar and the consumable environment every binary and app
//! reads its settings through.
//!
//! | [`parse`] | the value grammar — units, booleans, sentinels |
//! | [`env`] | the environment as a map consumed by reading, so a typo is caught |
//!
//! [`Env`] is the whole snapshot; an app gets only a [`env::Scope`] of it.

pub mod env;
pub mod parse;

use std::path::PathBuf;

pub use env::{Env, Pairs, Scope, Secret};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{var} is required but not set")]
    Missing { var: &'static str },

    #[error("{var}={value:?} is not valid: expected {expected}")]
    BadValue {
        var: &'static str,
        value: String,
        expected: String,
    },

    #[error("{var} is not a recognised setting{}", match suggestion {
        Some(s) => format!(" (did you mean {s}?)"),
        None => String::new(),
    })]
    Unknown {
        var: String,
        suggestion: Option<&'static str>,
    },

    #[error("{var} holds a value that is not valid UTF-8")]
    NonUtf8 { var: String },

    #[error("{prefix}{name}: set both {prefix}{name}{} and {prefix}{name}{}", halves.0, halves.1)]
    HalfPair {
        prefix: &'static str,
        name: String,
        halves: (&'static str, &'static str),
    },

    #[error("{var}: cannot use directory {path:?}: {source}")]
    Directory {
        var: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },

    /// An app's own refusal (or the list's), already rendered.
    #[error("{0}")]
    Other(String),

    #[error("{var}: rejected by zslg: {source}")]
    Rejected {
        var: &'static str,
        source: zslg::Error,
    },
}

impl ConfigError {
    pub fn bad_value(var: &'static str, value: &str, expected: impl Into<String>) -> Self {
        Self::BadValue {
            var,
            value: value.to_owned(),
            expected: expected.into(),
        }
    }
}
