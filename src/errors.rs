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
}
