// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

use std::{fs::File, io::BufReader};
pub mod models;

mod errors;

use crate::{errors::EngineError, models::TransactionInfo};

use csv::{ReaderBuilder, Trim};
use std::io::Read;

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
pub fn run_with_path(file_path: &str) -> Result<(), EngineError> {
    // inside an input file, transactions followed by other transactions are assumed to be
    // chronologically ordered, so we can process them in a single pass.
    let buff = BufReader::new(
        File::open(file_path).map_err(|err| EngineError::FileError { source: err })?,
    );

    for result in parse_transactions(buff) {
        match result {
            Ok(tx) => {
                println!("Processing transaction: {:#?}", tx);
            }
            Err(e) => {
                eprintln!("skipping unparseable row: {e}");
            }
        }
    }

    println!("File opened successfully, transactions readed...");
    Ok(())
}

/// Runs the payment engine using CLI arguments.
pub fn run() -> Result<(), EngineError> {
    let file_path = parse_file_path(std::env::args().nth(1))?;
    run_with_path(&file_path)
}

#[cfg(test)]
mod tests {
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
        let result = run_with_path(path);
        std::fs::remove_file(path).unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_with_path_file_not_found() {
        let result = run_with_path("/tmp/pecrab_nonexistent_file.csv");
        assert!(matches!(result, Err(EngineError::FileError { .. })));
    }
}
