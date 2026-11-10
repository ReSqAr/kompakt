//! The vocabulary the platform's capabilities share.
//!
//! ```text
//!   Error                 an app's error, boxed, crossing the seam (the runtime prints the chain)
//!   Shutdown / Stopped    a request to stop, and the proof one was honoured
//!   Job                   background work that ends only when told to (or fails)
//!   SigningKey            the process-wide secret, redacted in every Debug
//! ```
//!
//! The five capabilities (web, db, background, cli, metrics) are independent
//! siblings: anything two of them need sits here, beneath them.

mod error;
mod key;
mod stop;

pub use error::Error;
pub use key::SigningKey;
pub use stop::{Job, Shutdown, Stopped};
