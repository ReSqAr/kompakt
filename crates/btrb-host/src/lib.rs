//! The process machinery of an app list: what runs the apps once a binary
//! has said which they are.
//!
//! ```text
//!   settings   the environment → one validated Settings: each capability's slice
//!              (btrb-db, -web, -metrics, -cli), the apps', and the role defaults
//!   routes     /health, /ready and the startup gate, on an explicit FoldHealth
//!   metrics    the process's histograms, then the recorder and the /metrics listener
//!   serve      the server role, end to end (and what the listening roles share)
//!   work       the worker role: jobs and the CLI's socket
//!   standalone both on one node
//!   cli        one app command through the socket
//! ```
//!
//! Operational choices stay the binary's: the log subscriber and the
//! allocator are installed by `main`, never here.

mod cli;
mod metrics;
mod routes;
mod serve;
mod settings;
mod standalone;
mod work;

pub use cli::{cli_command, run_cli};

use std::error::Error;

/// The process shapes of an app list: which roles one process carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The web role on a replica node.
    Serve,
    /// The background role on a follower node, and the CLI's socket.
    Work,
    /// Both, on one replica node.
    Standalone,
}

/// Run `apps` in `role` until a signal, or until the fold or a job fails.
///
/// # Errors
/// A configuration, start-up or runtime failure; the process then exits
/// non-zero, so an orchestrator restarts it.
pub async fn run(apps: &[btrb_app::Entry], role: Role) -> Result<(), Box<dyn Error>> {
    match role {
        Role::Serve => serve::run_serve(apps).await,
        Role::Work => work::run_work(apps).await,
        Role::Standalone => standalone::run_standalone(apps).await,
    }
}
