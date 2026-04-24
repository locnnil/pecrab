// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

use thiserror::Error;

/// Represents the various errors that can occur in the payment engine.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Indicates invalid CLI argument.
    #[error("invalid CLI argument: {0}")]
    InvalidCliArgument(String),

    /// Indicates an error during CSV parsing.
    #[error("CSV parse error: {source}")]
    CsvParseError {
        #[from]
        source: csv::Error,
    },

    /// Indicates an error opening or reading the CSV file.
    #[error("file error: {source}")]
    FileError {
        #[from]
        source: std::io::Error,
    },

    /// Indicates an error initialising the on-disk transaction ledger.
    #[error("ledger init error: {0}")]
    LedgerInitError(String),

    /// A tokio task panicked or was cancelled.
    #[error("async task error: {0}")]
    TaskError(String),

    /// The tokio runtime could not be constructed.
    #[error("runtime error: {0}")]
    RuntimeError(String),

    /// A configuration value (typically from an environment variable) could
    /// not be parsed or was out of range.
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// The payment engine could not be constructed — typically because an
    /// environment variable that sizes its internal buffers was set to an
    /// invalid value.
    #[error("engine initialisation error: {0}")]
    EngineInitError(String),
}
