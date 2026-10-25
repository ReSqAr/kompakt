//! Value grammar for the `ZSLG_*` / `NVZN_*` environment: one scalar per
//! variable, each with a single unit suffix.
//!
//! ```text
//!   duration  90s  200ms  2min  1h  7d      suffix mandatory, exactly one
//!   bytes     5MB  256MiB  64KB  4096       suffix optional (bare = bytes)
//!   bool      true  false                   one spelling, not eight
//!   absent    off | none                    fall back to the crate default
//! ```
//!
//! Deliberately *not* supported: compound durations (`1h30m`), fractional
//! quantities, and decimal byte units. A suffix is read as binary whichever way
//! it is spelled — `MB` and `MiB` are both 1024² — because that distinction has
//! never been what someone meant when they typed a memory budget.
//!
//! Every function here is total and pure: it maps one `&str` to one value, or to
//! a [`ConfigError`] naming the variable, what it saw, and what it wanted. The
//! variable name is threaded through so a failure reads as a fix instruction
//! rather than as a parse trace.

use std::num::NonZeroUsize;
use std::time::Duration;

use zslg::ByteSize;

use super::ConfigError;

/// `"90s"` -> `("90", "s")`: the leading digits, then the rest, both trimmed.
fn split_suffix(raw: &str) -> (&str, &str) {
    let trimmed = raw.trim();
    let digits_end = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(digits_end);
    (digits, suffix.trim())
}

fn magnitude(
    var: &'static str,
    raw: &str,
    digits: &str,
    expected: &'static str,
) -> Result<u64, ConfigError> {
    if digits.is_empty() {
        return Err(ConfigError::bad_value(var, raw, expected));
    }
    digits
        .parse::<u64>()
        .map_err(|_| ConfigError::bad_value(var, raw, expected))
}

const DURATION_EXPECTED: &str =
    "an integer with one unit suffix: ms, s, min, h or d (e.g. `200ms`, `90s`, `2min`, `1h`, `7d`)";

/// A duration. The suffix is mandatory — a bare `5` is rejected rather than
/// guessed, because the fields it configures span `5ms` to `365d`, and a wrong
/// guess is a five-order-of-magnitude error on a node that starts up looking
/// healthy.
pub fn duration(var: &'static str, raw: &str) -> Result<Duration, ConfigError> {
    let (num, suffix) = split_suffix(raw);
    let n = magnitude(var, raw, num, DURATION_EXPECTED)?;
    let secs_per: u64 = match suffix.to_ascii_lowercase().as_str() {
        "ms" => return Ok(Duration::from_millis(n)),
        "s" | "sec" | "secs" => 1,
        "m" | "min" | "mins" => 60,
        "h" | "hr" | "hrs" => 60 * 60,
        "d" | "day" | "days" => 60 * 60 * 24,
        _ => return Err(ConfigError::bad_value(var, raw, DURATION_EXPECTED)),
    };
    // checked, not saturating: a duration that overflows is a typo, and pinning
    // it at ~584 billion years would start the node on a nonsense value.
    n.checked_mul(secs_per)
        .map(Duration::from_secs)
        .ok_or_else(|| ConfigError::bad_value(var, raw, DURATION_EXPECTED))
}

const BYTES_EXPECTED: &str =
    "an integer with an optional binary unit suffix: B, KB, MB, GB (e.g. `5MB`, `256MiB`, `4096`)";

/// A byte size. Every unit is binary (1024ⁿ); a bare integer is bytes, the one
/// place a missing suffix cannot be ambiguous.
pub fn bytes(var: &'static str, raw: &str) -> Result<ByteSize, ConfigError> {
    let (num, suffix) = split_suffix(raw);
    let n = magnitude(var, raw, num, BYTES_EXPECTED)?;
    let scale: u64 = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return Err(ConfigError::bad_value(var, raw, BYTES_EXPECTED)),
    };
    // Checked for `duration`'s reason: a wrapped budget stalls much later.
    n.checked_mul(scale)
        .map(ByteSize::from_bytes)
        .ok_or_else(|| ConfigError::bad_value(var, raw, BYTES_EXPECTED))
}

/// `true` or `false`, and nothing else. Accepting `1`/`yes`/`on` too would buy
/// nothing and cost the one word that has to stay unambiguous: `off` is how an
/// `Option` field is pinned back to the crate's derivation, so it must not also
/// be a way to write `false`.
///
/// Case still folds: a shell that upper-cases its exports said the same word.
pub fn boolean(var: &'static str, raw: &str) -> Result<bool, ConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConfigError::bad_value(var, raw, "`true` or `false`")),
    }
}

/// A plain count, for the knobs carrying no unit. `expected` names the range in
/// the field's own terms — a Rust type name is not what the operator typed at.
pub fn integer<T: std::str::FromStr>(
    var: &'static str,
    raw: &str,
    expected: &'static str,
) -> Result<T, ConfigError> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| ConfigError::bad_value(var, raw, expected))
}

/// A capacity. Separate from [`integer`] only so that zero — the one value a
/// channel depth may never take — explains itself.
pub fn nonzero(var: &'static str, raw: &str) -> Result<NonZeroUsize, ConfigError> {
    raw.trim().parse::<NonZeroUsize>().map_err(|_| {
        ConfigError::bad_value(var, raw, "a positive integer (zero is not a capacity)")
    })
}

/// Whether a value spells the *absence* of a value.
///
/// An empty string is **not** absence: a bare `FOO=` is far more often an
/// unresolved `${BAR}` than an intent, so it falls through to the scalar parsers
/// and fails there with the variable named.
#[must_use]
pub fn is_absent(raw: &str) -> bool {
    matches!(raw.trim().to_ascii_lowercase().as_str(), "off" | "none")
}

/// A 32-byte secret, written as 64 hexadecimal characters.
///
/// Hex rather than a passphrase on purpose. A free-form string invites one a
/// person chose, and this key is the only thing standing between a subdomain
/// cookie-writer and a forged CSRF token — it has to be 32 bytes from a random
/// source, and the format is what makes that the obvious thing to supply. The
/// error names the command that produces one.
pub fn signing_key(var: &'static str, raw: &str) -> Result<crate::SigningKey, ConfigError> {
    const EXPECTED: &str =
        "64 hexadecimal characters (32 bytes) — generate one with `openssl rand -hex 32`";

    let raw = raw.trim();
    if raw.len() != crate::SigningKey::BYTES.saturating_mul(2) {
        return Err(ConfigError::bad_value(var, raw, EXPECTED));
    }

    let mut bytes = [0u8; crate::SigningKey::BYTES];
    let (it, _) = raw.as_bytes().as_chunks::<2>();
    for (byte, pair) in bytes.iter_mut().zip(it) {
        let hex =
            core::str::from_utf8(pair).map_err(|_| ConfigError::bad_value(var, raw, EXPECTED))?;
        *byte =
            u8::from_str_radix(hex, 16).map_err(|_| ConfigError::bad_value(var, raw, EXPECTED))?;
    }

    Ok(crate::SigningKey::from_bytes(bytes))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    const V: &str = "ZSLG_TEST";

    #[test]
    fn durations_take_exactly_one_suffix() {
        assert_eq!(duration(V, "200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(duration(V, "90s").unwrap(), Duration::from_secs(90));
        assert_eq!(duration(V, "2min").unwrap(), Duration::from_secs(120));
        assert_eq!(duration(V, "1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(duration(V, "7d").unwrap(), Duration::from_secs(604_800));
        assert_eq!(duration(V, " 90 s ").unwrap(), Duration::from_secs(90));
    }

    #[test]
    fn a_duration_without_a_unit_is_an_error() {
        assert!(duration(V, "5").is_err());
        assert!(duration(V, "").is_err());
        assert!(duration(V, "s").is_err());
        assert!(duration(V, "1h30m").is_err());
        assert!(duration(V, "1.5s").is_err());
        assert!(duration(V, "-5s").is_err());
    }

    #[test]
    fn byte_units_are_binary_however_they_are_spelled() {
        assert_eq!(bytes(V, "5MB").unwrap(), ByteSize::from_mib(5));
        assert_eq!(bytes(V, "5MiB").unwrap(), ByteSize::from_mib(5));
        assert_eq!(bytes(V, "5mb").unwrap(), ByteSize::from_mib(5));
        assert_eq!(bytes(V, "64KB").unwrap(), ByteSize::from_kib(64));
        assert_eq!(bytes(V, "2GB").unwrap(), ByteSize::from_gib(2));
        // The one unambiguous bare number: no suffix means bytes.
        assert_eq!(bytes(V, "4096").unwrap(), ByteSize::from_bytes(4096));
        assert_eq!(bytes(V, "4096B").unwrap(), ByteSize::from_bytes(4096));
    }

    #[test]
    fn an_overflowing_quantity_is_an_error_not_a_saturation() {
        assert!(bytes(V, "99999999999999999999GB").is_err());
        assert!(duration(V, "99999999999999999999d").is_err());
    }

    #[test]
    fn a_boolean_has_exactly_one_spelling() {
        assert!(boolean(V, "true").unwrap());
        assert!(!boolean(V, "false").unwrap());
        // Case is not a different word.
        assert!(boolean(V, "TRUE").unwrap());
        assert!(!boolean(V, " False ").unwrap());

        for rejected in ["1", "0", "yes", "no", "on", "y", "n", "maybe", ""] {
            assert!(boolean(V, rejected).is_err(), "{rejected}");
        }
    }

    #[test]
    fn off_is_absence_and_never_false() {
        assert!(is_absent("off"));
        assert!(boolean(V, "off").is_err());
    }

    #[test]
    fn only_off_and_none_spell_absence() {
        assert!(is_absent("off"));
        assert!(is_absent("NONE"));
        assert!(is_absent("  off  "));
        assert!(!is_absent(""));
        assert!(!is_absent("  "));
        assert!(!is_absent("0"));
    }

    #[test]
    fn an_error_names_the_variable_and_the_value() {
        let err = duration("ZSLG_SNAPSHOT_INTERVAL", "5").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("ZSLG_SNAPSHOT_INTERVAL"), "{text}");
        assert!(text.contains('5'), "{text}");
        assert!(text.contains("ms"), "{text}");
    }
}
