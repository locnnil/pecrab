// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

// Integration test helpers are not annotated with #[test], so the
// allow-*-in-tests .clippy.toml options don't apply to them.
#![allow(clippy::panic, clippy::unwrap_used)]

use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use rust_decimal::Decimal;
use std::str::FromStr;

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
}

#[derive(Debug, PartialEq, Eq)]
struct NormalisedRow {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

fn parse_output_csv(csv_text: &str) -> HashMap<u16, NormalisedRow> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(csv_text.as_bytes());

    reader
        .records()
        .map(|result| {
            let record = result.expect("malformed CSV record in output");
            let client: u16 = record[0].trim().parse().expect("client must be u16");
            let available = Decimal::from_str(record[1].trim()).expect("available must be decimal");
            let held = Decimal::from_str(record[2].trim()).expect("held must be decimal");
            let total = Decimal::from_str(record[3].trim()).expect("total must be decimal");
            let locked: bool = record[4].trim().parse().expect("locked must be bool");
            (
                client,
                NormalisedRow {
                    client,
                    available,
                    held,
                    total,
                    locked,
                },
            )
        })
        .collect()
}

/// Discovers all input/output pairs in `tests/data/`.
///
/// An input file matches `sample_<N>_<description>.csv` where the name is
/// NOT `sample_<N>_out.csv`. A pair is only valid when the corresponding
/// `sample_<N>_out.csv` also exists. Panics if any input has no matching
/// output or if the directory is unreadable.
fn discover_samples() -> Vec<(PathBuf, PathBuf)> {
    let dir = data_dir();

    let mut inputs: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {:?}: {e}", dir))
        .filter_map(|e| {
            let path = e.ok()?.path();
            let name = path.file_name()?.to_str()?;
            if name.starts_with("sample_") && !name.ends_with("_out.csv") && name.ends_with(".csv")
            {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    assert!(
        !inputs.is_empty(),
        "no sample input files found in {:?}",
        dir
    );

    inputs.sort();

    let mut missing_outputs = Vec::new();

    let pairs: Vec<(PathBuf, PathBuf)> = inputs
        .into_iter()
        .map(|input| {
            // Extract the `N` from `sample_N_<desc>.csv`
            let stem = input.file_stem().unwrap().to_str().unwrap();
            // stem is e.g. "sample_01_basic_deposits_only"
            let num_part = stem.split('_').nth(1).unwrap_or("");
            let expected = dir.join(format!("sample_{}_out.csv", num_part));

            if !expected.exists() {
                missing_outputs.push(format!("{:?} -> {:?} (missing)", input, expected));
            }

            (input, expected)
        })
        .collect();

    if !missing_outputs.is_empty() {
        panic!(
            "{} input file(s) have no matching output:\n  {}",
            missing_outputs.len(),
            missing_outputs.join("\n  ")
        );
    }

    pairs
}

fn run_sample_parallel(input: &Path, expected_path: &Path) {
    run_sample_with(input, expected_path, |r, buf| {
        pecrab::run_with_writer_parallel(r, buf)
    });
}

fn run_sample_with<F>(input: &Path, expected_path: &Path, engine: F)
where
    F: FnOnce(BufReader<File>, &mut Vec<u8>) -> Result<(), pecrab::EngineError>,
{
    let reader = BufReader::new(
        File::open(input).unwrap_or_else(|e| panic!("cannot open {:?}: {e}", input)),
    );

    let mut output_buf: Vec<u8> = Vec::new();
    engine(reader, &mut output_buf).unwrap_or_else(|e| panic!("engine error on {:?}: {e}", input));

    let actual_csv = String::from_utf8(output_buf).expect("engine output is not valid UTF-8");
    let expected_csv = std::fs::read_to_string(expected_path)
        .unwrap_or_else(|e| panic!("cannot read {:?}: {e}", expected_path));

    let actual = parse_output_csv(&actual_csv);
    let expected = parse_output_csv(&expected_csv);

    let mut errors: Vec<String> = Vec::new();

    for (client, exp) in &expected {
        match actual.get(client) {
            None => errors.push(format!(
                "  client {client}: missing from output (expected {exp:?})"
            )),
            Some(act) if act != exp => errors.push(format!(
                "  client {client}:\n    expected: {exp:?}\n    actual:   {act:?}"
            )),
            _ => {}
        }
    }
    for client in actual.keys() {
        if !expected.contains_key(client) {
            errors.push(format!(
                "  client {client}: present in output but not expected"
            ));
        }
    }

    if !errors.is_empty() {
        panic!(
            "{:?} failed ({} divergence(s)):\n{}",
            input.file_name().unwrap(),
            errors.len(),
            errors.join("\n")
        );
    }
}

#[test]
fn all_samples_parallel() {
    let pairs = discover_samples();

    let mut failures: Vec<String> = Vec::new();

    for (input, expected) in &pairs {
        eprintln!(
            "running sample: {}",
            input.file_name().unwrap().to_str().unwrap()
        );
        let result = std::panic::catch_unwind(|| run_sample_parallel(input, expected));
        if let Err(e) = result {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".to_string());
            failures.push(format!(
                "{}: {}",
                input.file_name().unwrap().to_str().unwrap(),
                msg
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{}/{} sample(s) failed:\n\n{}",
            failures.len(),
            pairs.len(),
            failures.join("\n\n")
        );
    }

    println!("{} sample(s) passed (parallel)", pairs.len());
}
