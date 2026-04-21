// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

use std::{fs::File, io::BufReader};
pub mod models;

mod errors;

use crate::errors::EngineError;

// Run core logic of the application
pub fn run() -> Result<(), EngineError> {
    let file_path = std::env::args().nth(1).ok_or_else(|| {
        EngineError::InvalidCliArgument("CSV filename missing in cli argument".to_owned())
    })?;

    println!("Processing file: {}", file_path);

    // inside an input file, transactions followed by other transations are assumed to be
    // chronologically ordered, so we can process them in a single pass.
    let buff = BufReader::new(
        File::open(file_path).map_err(|err| EngineError::FileError(err.to_string()))?,
    );

    println!("File opened successfully, starting to process transactions...");
    dbg!("buffer: {:#?}", buff);

    Ok(())
}
