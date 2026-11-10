//! The CLI capability: how an operator command reaches a running worker, and what a command may do to a terminal.
//!
//! ```text
//!   cli   Remote::call(Request) ─▶ frame ─▶ unix socket (0600) ─▶ AdminSocket ─▶ Handler
//!   Console                         print, prompt, read stdin: the real terminal, or a scripted one in tests
//! ```
//!
//! The runtime knows nothing of an app's calls: each app declares its own `Request`/`Response`. The socket's
//! filesystem permissions are the boundary: whoever can open it is the operator.

pub mod admin;
pub mod console;
pub mod settings;

pub use admin::{AdminSocket, Handler, NoCalls, Remote, Served, fingerprint};
pub use console::{Console, ScriptedConsole, StdConsole};
