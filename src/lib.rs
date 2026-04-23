// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

use std::{fs::File, io::BufReader};
pub mod models;

pub use engine::Payments;
pub use errors::EngineError;
pub use parallel::run_with_writer_parallel;
mod engine;
mod env;
mod errors;
mod parallel;

use crate::models::TransactionInfo;

use csv::{ReaderBuilder, Trim};
use std::io::{Read, Write};

pub fn parse_transactions<R: Read>(
    reader: R,
) -> impl Iterator<Item = Result<TransactionInfo, EngineError>> {
    ReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        .from_reader(reader)
        .into_deserialize()
        .map(|result| result.map_err(Into::into))
}

/// Resolves the CSV file path from an optional CLI argument.
fn parse_file_path(arg: Option<String>) -> Result<String, EngineError> {
    arg.ok_or_else(|| {
        EngineError::InvalidCliArgument("CSV filename missing in cli argument".to_owned())
    })
}

/// Processes transactions from the CSV file at `file_path`.
pub fn run_with_writer<R: Read, W: Write>(reader: R, writer: W) -> Result<(), EngineError> {
    let mut engine = Payments::new().map_err(|e| EngineError::LedgerInitError(e.to_string()))?;

    for result in parse_transactions(reader) {
        match result {
            Ok(tx) => {
                if let Err(e) = engine.apply(tx) {
                    eprintln!("skipping malformed tx: {e}");
                }
            }
            Err(e) => {
                eprintln!("skipping unparseable row: {e}");
            }
        }
    }

    let mut csv_writer = csv::Writer::from_writer(writer);
    for account in engine.accounts() {
        csv_writer
            .serialize(account)
            .map_err(|e| EngineError::CsvParseError { source: e })?;
    }
    csv_writer.flush()?;

    Ok(())
}

/// Runs the payment engine using CLI arguments.
pub fn run() -> Result<(), EngineError> {
    let file_path = parse_file_path(std::env::args().nth(1))?;

    // inside an input file, transactions followed by other transactions are assumed to be
    // chronologically ordered, so we can process them in a single pass.
    let buff = BufReader::new(
        File::open(file_path).map_err(|err| EngineError::FileError { source: err })?,
    );

    run_with_writer_parallel(buff, std::io::stdout())
}

#[cfg(test)]
mod lib_tests {
    use super::*;

    #[test]
    fn test_parse_file_path_with_arg() {
        let result = parse_file_path(Some("transactions.csv".to_owned()));
        assert_eq!(result.unwrap(), "transactions.csv");
    }

    #[test]
    fn test_parse_file_path_missing_arg() {
        let result = parse_file_path(None);
        assert!(matches!(result, Err(EngineError::InvalidCliArgument(_))));
    }

    #[test]
    fn test_run_with_path_file_exists() {
        let path = "/tmp/pecrab_test_existing.csv";
        std::fs::write(path, "type,client,tx,amount\n").unwrap();
        let file = File::open(path).unwrap();
        let result = run_with_writer(BufReader::new(file), std::io::sink());
        std::fs::remove_file(path).unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_path_file_not_found() {
        let result = File::open("/tmp/pecrab_nonexistent_file.csv")
            .map_err(|err| EngineError::FileError { source: err });
        assert!(matches!(result, Err(EngineError::FileError { .. })));
    }
}
