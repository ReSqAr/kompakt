//! The CLI capability's slice of the environment.
//!
//! ```text
//!   BTRB_CLI_SOCKET   the worker's admin socket: required, and not empty
//! ```

use btrb_config::{ConfigError, Env};
use std::path::PathBuf;

pub fn cli_socket(env: &mut Env) -> Result<PathBuf, ConfigError> {
    let raw = env.required("BTRB_CLI_SOCKET")?;
    if raw.trim().is_empty() {
        return Err(ConfigError::Other(
            "BTRB_CLI_SOCKET is empty: it is the path of the worker's socket".to_owned(),
        ));
    }
    Ok(PathBuf::from(raw))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn the_cli_socket_is_required_and_not_empty() {
        let mut env = Env::from_pairs(&[]);
        assert!(cli_socket(&mut env).is_err());
        let mut env = Env::from_pairs(&[("BTRB_CLI_SOCKET", " ")]);
        assert!(cli_socket(&mut env).is_err());
        let mut env = Env::from_pairs(&[("BTRB_CLI_SOCKET", "/run/host/cli.sock")]);
        assert_eq!(
            cli_socket(&mut env).unwrap(),
            PathBuf::from("/run/host/cli.sock")
        );
    }
}
