//! The web capability's slice of the environment.
//!
//! ```text
//!   BTRB_INSECURE        no `Secure` cookie attribute, no HSTS (development only)
//!   BTRB_LISTEN_ADDR     where the HTTP server binds (port 0 allowed: nothing advertises it)
//! ```

use crate::CookiePolicy;
use btrb_config::{ConfigError, Env, parse};
use std::net::{Ipv4Addr, SocketAddr};

pub const DEFAULT_LISTEN_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);

/// `BTRB_LISTEN_ADDR`, defaulting to loopback.
pub fn http_listen_addr(env: &mut Env) -> Result<SocketAddr, ConfigError> {
    let mut addr = DEFAULT_LISTEN_ADDR;
    env.set(&mut addr, "BTRB_LISTEN_ADDR", parse::socket_addr)?;
    Ok(addr)
}

/// `BTRB_INSECURE` turns off the `Secure` cookie attribute so a plain-HTTP
/// `localhost` run can hold a session. It defaults to off, and it is loud when
/// on: a deployment that reaches this branch by accident is serving session
/// cookies a plain-HTTP hop can read, and the warning is the only thing that
/// will say so.
///
/// It is read here rather than in the app so the `BTRB_` prefix brings it under
/// `Env::finish`'s unknown-variable check. Without that a typo reads as "not
/// set" — safe for this variable, and not for the next one.
pub fn cookie_policy(env: &mut Env) -> Result<CookiePolicy, ConfigError> {
    let mut insecure = false;
    env.set(&mut insecure, "BTRB_INSECURE", parse::boolean)?;

    if insecure {
        tracing::warn!(
            "BTRB_INSECURE is set: cookies will not carry the Secure attribute, so any \
             plain-HTTP hop can read a session. Development only."
        );
        Ok(CookiePolicy::AllowPlainHttp)
    } else {
        Ok(CookiePolicy::RequireHttps)
    }
}
