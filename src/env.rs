// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Environment-variable helpers for runtime configuration.

use anyhow::{Context, Result, bail};
use std::path::PathBuf;

/// Environment variable that overrides the directory used to host the redb
/// ledger file. Useful for steering the ledger onto a specific scratch volume
/// (e.g. an NVMe disk) in production.
const LEDGER_DIR_ENV: &str = "PECRAB_LEDGER_DIR";

/// Resolve the directory in which to place the ephemeral ledger file.
///
/// Resolution order:
/// 1. `PECRAB_LEDGER_DIR` environment variable — explicit operator override.
/// 2. `/var/tmp` — per the FHS, must be preserved across reboots, so distros
///    conventionally back it with disk (unlike `/tmp`, which is `tmpfs` on
///    most modern systemd-default installations).
/// 3. [`std::env::temp_dir`] — last resort. Logs a warning to stderr because
///    this is typically `/tmp`, which on many distributions is RAM-backed —
///    defeating the point of spilling the ledger to disk.
pub fn resolve_ledger_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os(LEDGER_DIR_ENV) {
        return PathBuf::from(dir);
    }

    let var_tmp = PathBuf::from("/var/tmp");
    if var_tmp.is_dir() {
        return var_tmp;
    }

    let fallback = std::env::temp_dir();
    eprintln!(
        "warning: /var/tmp is not available; falling back to {} for the ledger file. \
         This directory may be tmpfs (RAM-backed), which would defeat the purpose of \
         spilling the ledger to disk. Set {} to a disk-backed directory to silence this warning.",
        fallback.display(),
        LEDGER_DIR_ENV,
    );
    fallback
}

// ---------------------------------------------------------------------------
// SI byte parsing (shared by global memory budget)
// ---------------------------------------------------------------------------

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
        .with_context(|| format!("invalid numeric prefix '{num_str}'"))?;

    value
        .checked_mul(multiplier)
        .context("byte value overflows u64")
}

// ---------------------------------------------------------------------------
// Global memory budget configuration
// ---------------------------------------------------------------------------

/// Environment variable that caps the **aggregate** memory held in the pending
/// buffers of all actors combined.
///
/// [`TX_MEMORY_ENV`] sizes a *single* actor; this variable bounds the *sum*
/// across the actor pool. Without it, N actors × `TX_MEMORY_ENV` can exceed
/// physical RAM and invite the OOM killer. Accepts the same SI-suffixed
/// format as [`TX_MEMORY_ENV`] (see [`parse_si_bytes`]).
pub const GLOBAL_MEMORY_ENV: &str = "PECRAB_GLOBAL_MEMORY_LIMIT";

/// Default aggregate budget when [`GLOBAL_MEMORY_ENV`] is unset (2 GB).
///
/// Sized to comfortably hold the full u16 client space (65 535 clients) with
/// a reasonable per-client deposit history before forcing disk spills.
/// Operators may override via [`GLOBAL_MEMORY_ENV`].
pub const DEFAULT_GLOBAL_MEMORY_LIMIT: usize = 2_000_000_000;

/// Read [`GLOBAL_MEMORY_ENV`] and return the aggregate budget in bytes.
///
/// Falls back to `default` when the variable is unset.
///
/// # Errors
///
/// Returns an error if the variable is set but cannot be parsed, or if the
/// resulting byte count overflows `usize`.
pub fn global_mem_limit_from_env(default: usize) -> Result<usize> {
    let bytes = match std::env::var(GLOBAL_MEMORY_ENV) {
        Ok(val) => parse_si_bytes(&val)
            .with_context(|| format!("failed to parse {GLOBAL_MEMORY_ENV}={val:?}"))?,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(s)) => {
            bail!("{GLOBAL_MEMORY_ENV} is not valid UTF-8: {s:?}")
        }
    };

    bytes
        .try_into()
        .with_context(|| format!("{GLOBAL_MEMORY_ENV} value overflows usize"))
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
