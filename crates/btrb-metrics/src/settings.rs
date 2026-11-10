//! The metrics capability's slice of the environment.
//!
//! ```text
//!   BTRB_METRICS_LISTEN_ADDR   where `/metrics` is served; `off`/`none` turns export off
//! ```

use btrb_config::{ConfigError, Env, parse};
use std::net::{Ipv4Addr, SocketAddr};

pub const DEFAULT_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 9100);

/// `BTRB_METRICS_LISTEN_ADDR`: `default` when unset, `None` when pinned off.
pub fn listen_addr(
    env: &mut Env,
    default: Option<SocketAddr>,
) -> Result<Option<SocketAddr>, ConfigError> {
    let mut addr = default;
    env.set_optional(&mut addr, "BTRB_METRICS_LISTEN_ADDR", parse::socket_addr)?;
    Ok(addr)
}
