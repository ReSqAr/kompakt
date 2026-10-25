//! The CLI role: one command of one app, asked of the process that works.
//!
//! ```text
//!   argv ──▶ clap: an app's name, then its own subcommands (from the list)
//!        ──▶ the app's `command` through the terminal's Console, its calls
//!            over the socket (BTRB_CLI_SOCKET); nothing is booted
//! ```
//!
//! Run it where the process runs: the socket is a file of its own, open to its
//! user alone.

use clap::{ArgMatches, Command};
use std::error::Error;

/// The apps as a command line: `<name> app album create …`.
#[must_use]
pub fn cli_command(name: &'static str, entries: &[crate::Entry]) -> Command {
    let mut cli = Command::new(name)
        .about("Commands for the apps of this process, asked of the process that works")
        .infer_subcommands(true)
        .subcommand_required(true)
        .arg_required_else_help(true);
    for entry in entries {
        let app = Command::new(entry.name())
            .subcommand_required(true)
            .arg_required_else_help(true);
        cli = cli.subcommand(entry.augment(app));
    }
    cli
}

/// Run the app command in `matches` (parsed from [`cli_command`]) against the
/// process listening on `BTRB_CLI_SOCKET`.
///
/// # Errors
/// The command's own error, or a missing socket variable.
pub async fn run_cli(entries: &[crate::Entry], matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let Some((name, args)) = matches.subcommand() else {
        return Err("no command".into());
    };
    let entry = entries
        .iter()
        .find(|e| e.name() == name)
        .ok_or_else(|| format!("no app named {name}"))?;
    let socket = super::settings::cli_socket_from_env()?;

    let mut console = crate::StdConsole;
    entry.command(args, &mut console, &socket).await?;
    Ok(())
}
