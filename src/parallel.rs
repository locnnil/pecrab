// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Parallel payment engine: actor-per-client model.
//!
//! # Architecture
//!
//! ```text
//!   CSV reader (spawn_blocking)
//!         │  Result<TransactionInfo>
//!         ▼
//!     pipe channel
//!         │
//!     dispatcher (async loop)  ← routes by client_id
//!         ├──► Payments actor task (client 1)
//!         ├──► Payments actor task (client 2)
//!         └──► Payments actor task (client N)
//! ```
//!
//! The CSV reader runs on a blocking thread ([`tokio::task::spawn_blocking`])
//! and feeds rows into an unbounded pipe channel. The dispatcher receives rows
//! and routes each one by `client_id` to a dedicated [`Payments`] actor task,
//! spawning a new task the first time a client is encountered.
//!
//! Each actor owns one [`Payments`] instance. Because every CSV row — including
//! `dispute`, `resolve`, and `chargeback` — carries a `client_id`, the
//! dispatcher always knows the destination without any shared lookup table.
//!
//! Dropping all actor senders (when the dispatch loop exits) closes each
//! actor's receive channel, causing its loop to terminate and the task to
//! resolve with the final account snapshots.
//!
//! # Memory
//!
//! [`Payments::new`] reads [`crate::env::TX_MEMORY_ENV`] to size its pending
//! buffer. Each actor reads the variable once at creation time, so all actors
//! share the same per-actor budget. The redb spill file is created lazily by
//! each actor only if it actually overflows its buffer.

use std::collections::HashMap;
use std::io::{Read, Write};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use crate::engine::Payments;
use crate::errors::EngineError;
use crate::models::{Account, TransactionInfo};
use crate::parse_transactions;

/// Run one [`Payments`] engine as an actor, draining `rx` until it is closed.
///
/// Returns all accounts seen by this actor (one per client in practice, since
/// the dispatcher routes by `client_id`).
async fn run_actor(mut rx: UnboundedReceiver<TransactionInfo>) -> Vec<Account> {
    let mut engine = match Payments::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("failed to initialise actor engine: {e}");
            // Drain the channel so the dispatcher is not blocked.
            while rx.recv().await.is_some() {}
            return Vec::new();
        }
    };
    while let Some(tx) = rx.recv().await {
        if let Err(e) = engine.apply(tx) {
            eprintln!("skipping malformed tx: {e}");
        }
    }
    engine.accounts().collect()
}

/// Process transactions using one [`Payments`] actor per client, writing the
/// final account states as CSV to `writer`.
///
/// Internally creates a multi-threaded tokio runtime. Must **not** be called
/// from within an existing tokio runtime context.
pub fn run_with_writer_parallel<R: Read + Send + 'static, W: Write>(
    reader: R,
    writer: W,
) -> Result<(), EngineError> {
    tokio::runtime::Builder::new_multi_thread()
        .build()
        .map_err(|e| EngineError::RuntimeError(e.to_string()))?
        .block_on(dispatch(reader, writer))
}

async fn dispatch<R: Read + Send + 'static, W: Write>(
    reader: R,
    writer: W,
) -> Result<(), EngineError> {
    let mut actor_senders: HashMap<u16, UnboundedSender<TransactionInfo>> = HashMap::new();
    let mut handles: Vec<JoinHandle<Vec<Account>>> = Vec::new();

    // Pipe between the blocking CSV reader and this async dispatch loop.
    let (pipe_tx, mut pipe_rx) = unbounded_channel::<Result<TransactionInfo, EngineError>>();

    let reader_handle = tokio::task::spawn_blocking(move || {
        for result in parse_transactions(reader) {
            // Stop early if the dispatch loop has exited (receiver dropped).
            if pipe_tx.send(result).is_err() {
                break;
            }
        }
    });

    while let Some(result) = pipe_rx.recv().await {
        match result {
            Ok(tx) => {
                let client = tx.client;
                let sender = actor_senders.entry(client).or_insert_with(|| {
                    let (s, r) = unbounded_channel();
                    // Spawn a new actor task for this client.
                    handles.push(tokio::spawn(run_actor(r)));
                    s
                });
                if sender.send(tx).is_err() {
                    eprintln!("actor for client {client} exited unexpectedly");
                }
            }
            Err(e) => eprintln!("skipping unparseable row: {e}"),
        }
    }

    reader_handle
        .await
        .map_err(|e| EngineError::TaskError(e.to_string()))?;

    // Closing all senders signals each actor that no more transactions are
    // coming; their recv loops drain and the tasks resolve.
    drop(actor_senders);

    let mut csv_writer = csv::Writer::from_writer(writer);
    for handle in handles {
        let accounts = handle
            .await
            .map_err(|e| EngineError::TaskError(e.to_string()))?;
        for account in accounts {
            csv_writer
                .serialize(account)
                .map_err(|e| EngineError::CsvParseError { source: e })?;
        }
    }
    csv_writer.flush()?;

    Ok(())
}
