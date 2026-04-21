// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;

fn deserialize_decimal_4dp<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize as Option<String> first — avoids f64 precision loss entirely
    let opt: Option<String> = Option::deserialize(deserializer)?;

    match opt {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => {
            let d = Decimal::from_str(s.trim())
                .map_err(|e| serde::de::Error::custom(format!("invalid decimal '{}': {}", s, e)))?;

            // Normalize to exactly 4 decimal places
            // dp() returns current scale; round_dp truncates/pads to target
            let normalized = d.round_dp(4);
            Ok(Some(normalized))
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TransactionInfo {
    #[serde(rename = "type")]
    pub type_tx: TransactionType,
    pub client: u16,
    pub tx: u32,
    #[serde(deserialize_with = "deserialize_decimal_4dp")]
    pub amount: Option<Decimal>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

fn serialize_decimal_4dp<S>(val: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // TODO: Check the round stategy, search how ATM round to 4dp.

    // round_dp_with_strategy ensures trailing zeros are kept in Display
    let normalized = val.round_dp(4);
    serializer.serialize_str(&format!("{:.4}", normalized))
}

#[derive(Debug, Serialize)]
pub struct Account {
    pub client: u16,
    #[serde(serialize_with = "serialize_decimal_4dp")]
    pub available: Decimal,
    #[serde(serialize_with = "serialize_decimal_4dp")]
    pub held: Decimal,
    #[serde(serialize_with = "serialize_decimal_4dp")]
    pub total: Decimal,
    pub locked: bool,
}
