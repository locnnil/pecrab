// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Environment-variable helpers for runtime configuration.

use anyhow::{Context, Result, bail};

// ---------------------------------------------------------------------------
// Pending-buffer configuration
// ---------------------------------------------------------------------------

/// Environment variable that controls the memory budget for the pending deposit buffer.
///
/// Accepts a byte count with an optional SI suffix (powers of 1 000, not 1 024):
/// `"512"` (bytes), `"256K"` / `"256KB"`, `"500M"` / `"500MB"`,
/// `"1G"` / `"1GB"`, `"2T"` / `"2TB"`. Suffixes are case-insensitive.
///
/// When unset the engine falls back to the `default` passed to
/// [`max_pending_from_env`].
pub const TX_MEMORY_ENV: &str = "PECRAB_TX_MEMORY_MAX";

/// Parse a human-readable byte count with an optional SI suffix.
///
/// SI prefixes use powers of 1 000 (not 1 024):
///
/// | Suffix (case-insensitive) | Multiplier        |
/// |---------------------------|-------------------|
/// | *(none)*                  | 1                 |
/// | `K` / `KB`                | 1 000             |
/// | `M` / `MB`                | 1 000 000         |
/// | `G` / `GB`                | 1 000 000 000     |
/// | `T` / `TB`                | 1 000 000 000 000 |
///
/// # Errors
///
/// Returns an error if the numeric part cannot be parsed as `u64` or if the
/// resulting byte count overflows `u64`.
pub fn parse_si_bytes(s: &str) -> Result<u64> {
    let s = s.trim();

    // Strip a trailing 'B' / 'b' (the "bytes" indicator, e.g. "MB" → "M").
    let s = if s.ends_with(['B', 'b']) {
        &s[..s.len() - 1]
    } else {
        s
    };

    let (multiplier, num_str) = match s.chars().last() {
        Some('K' | 'k') => (1_000u64, &s[..s.len() - 1]),
        Some('M' | 'm') => (1_000_000u64, &s[..s.len() - 1]),
        Some('G' | 'g') => (1_000_000_000u64, &s[..s.len() - 1]),
        Some('T' | 't') => (1_000_000_000_000u64, &s[..s.len() - 1]),
        _ => (1u64, s),
    };

    let value: u64 = num_str
        .trim()
        .parse()
        .with_context(|| format!("invalid number in {TX_MEMORY_ENV}: '{num_str}'"))?;

    value
        .checked_mul(multiplier)
        .with_context(|| format!("{TX_MEMORY_ENV} value overflows u64"))
}

/// Read [`TX_MEMORY_ENV`] and compute the maximum number of pending entries.
///
/// Divides the configured byte budget by `entry_size` (the memory cost of one
/// key-value pair in the pending buffer, computed by the caller with
/// `size_of`). Falls back to `default` when the variable is unset. Always
/// returns at least 1.
///
/// # Errors
///
/// Returns an error if the variable is set but cannot be parsed.
pub fn max_pending_from_env(entry_size: usize, default: usize) -> Result<usize> {
    let bytes = match std::env::var(TX_MEMORY_ENV) {
        Ok(val) => parse_si_bytes(&val)
            .with_context(|| format!("failed to parse {TX_MEMORY_ENV}={val:?}"))?,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(s)) => {
            bail!("{TX_MEMORY_ENV} is not valid UTF-8: {s:?}")
        }
    };

    let max: usize = (bytes / entry_size as u64)
        .try_into()
        .context("computed max_pending overflows usize")?;

    // Guard against a budget so small that the engine flushes on every deposit.
    Ok(max.max(1))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_si_bytes_plain_number() {
        assert_eq!(parse_si_bytes("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_si_bytes_kilobytes() {
        assert_eq!(parse_si_bytes("1K").unwrap(), 1_000);
        assert_eq!(parse_si_bytes("1KB").unwrap(), 1_000);
        assert_eq!(parse_si_bytes("1kb").unwrap(), 1_000);
    }

    #[test]
    fn parse_si_bytes_megabytes() {
        assert_eq!(parse_si_bytes("500M").unwrap(), 500_000_000);
        assert_eq!(parse_si_bytes("500MB").unwrap(), 500_000_000);
        assert_eq!(parse_si_bytes("500mb").unwrap(), 500_000_000);
    }

    #[test]
    fn parse_si_bytes_gigabytes() {
        assert_eq!(parse_si_bytes("2G").unwrap(), 2_000_000_000);
        assert_eq!(parse_si_bytes("2GB").unwrap(), 2_000_000_000);
    }

    #[test]
    fn parse_si_bytes_terabytes() {
        assert_eq!(parse_si_bytes("1T").unwrap(), 1_000_000_000_000);
        assert_eq!(parse_si_bytes("1TB").unwrap(), 1_000_000_000_000);
    }

    #[test]
    fn parse_si_bytes_invalid_returns_err() {
        assert!(parse_si_bytes("abc").is_err());
        assert!(parse_si_bytes("MB").is_err());
    }
}
