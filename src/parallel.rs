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
//! Every actor shares a single [`GlobalMemBudget`] sized by
//! [`crate::env::GLOBAL_MEMORY_ENV`] (default 2 GB). The dispatcher
//! constructs the budget once and hands each actor a clone of the `Arc`.
//! Each actor's post-insert flush threshold is derived live as
//! `global_limit / actor_count / entry_size`, so the sum of all in-memory
//! buffers stays within the hard ceiling regardless of how many clients are
//! active. Every pending-buffer insertion reserves against the budget and
//! every flush releases the bytes drained.
//!
//! The redb spill file remains lazy and per-actor — only actors that actually
//! overflow their buffer touch disk.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use crate::engine::Payments;
use crate::env;
use crate::errors::EngineError;
use crate::mem_budget::GlobalMemBudget;
use crate::models::{Account, TransactionInfo};
use crate::parse_transactions;

/// Run one [`Payments`] engine as an actor, draining `rx` until it is closed.
///
/// `budget` is the shared aggregate memory tracker; every actor holds a clone
/// of the same `Arc` so reservations contend on one counter.
///
/// Returns all accounts seen by this actor (one per client in practice, since
/// the dispatcher routes by `client_id`).
async fn run_actor(
    mut rx: UnboundedReceiver<TransactionInfo>,
    budget: Arc<GlobalMemBudget>,
) -> Vec<Account> {
    let mut engine = match Payments::new(budget) {
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
    // Aggregate memory ceiling shared by every actor. Constructed once here
    // and cloned into each spawned actor so reservations contend on a single
    // atomic counter (see `crate::mem_budget`).
    let global_limit = env::global_mem_limit_from_env(env::DEFAULT_GLOBAL_MEMORY_LIMIT)
        .map_err(|e| EngineError::ConfigError(e.to_string()))?;
    let budget = Arc::new(GlobalMemBudget::new(global_limit));

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
                    // Spawn a new actor task for this client, handing it a
                    // clone of the shared budget Arc.
                    handles.push(tokio::spawn(run_actor(r, Arc::clone(&budget))));
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
