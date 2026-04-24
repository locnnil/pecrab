// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Payment engine core.
//!
//! # Architecture
//!
//! ## Storage
//! Deposits are held in an insertion-ordered in-memory buffer ([`IndexMap`]).
//! When the buffer reaches the actor's fair share of the global budget a
//! temporary [`redb`] database is created **lazily** and the oldest 10 % of
//! entries are evicted to it. Subsequent overflows reuse the same file. This
//! bounds heap usage — allowing the full u32 transaction-ID space to be
//! processed — while paying zero disk cost for actors that stay within budget.
//! Commits use [`Durability::None`] (no fsync) because the backing file is
//! ephemeral and crash recovery is not required.
//!
//! ## Event sourcing
//! Every [`TransactionInfo`] that arrives is an immutable event. The engine never
//! mutates past events; it only appends to the deposit ledger and applies derived
//! changes to `accounts`.
//!
//! * `tx_ledger` — canonical record of every **deposit** (the only disputable
//!   transaction type). Backed by redb when spilled; keyed by the global tx ID.
//! * `accounts` — derived state rebuilt by replaying events in order.
//!
//! Because transactions arrive chronologically (per spec), a single forward pass
//! is sufficient; no replay from scratch is needed after each event.
//!
//! ## Per-account state machine
//! Each account is modelled as [`AccountFsm`], which wraps an [`AccountState`]
//! enum:
//!
//! ```text
//!          deposit / withdrawal / dispute / resolve / chargeback
//!                  ┌──────────────────────────────┐
//!                  ▼                              │  (all except chargeback)
//!               Active ──── chargeback ────► Locked
//! ```
//!
//! Any mutating operation on a `Locked` account is silently ignored, because once
//! a chargeback happens the account should be immediately frozen.
//!
//! ## Pending-buffer eviction
//! Deposits are buffered in an [`IndexMap`] (insertion-ordered). When the buffer
//! reaches the actor's fair share of the global budget — computed dynamically by
//! [`crate::mem_budget::GlobalMemBudget::suggested_per_actor_cap`] — the
//! **oldest 10 %** are evicted to the lazily-created redb file in a single
//! write transaction. The remaining 90 % stay in memory so recent deposits remain
//! fast to look up. Dispute / resolve / chargeback handlers always check the
//! in-memory buffer first, then fall through to the on-disk ledger.
//!
//! ## Aggregate memory budget
//! Each actor's flush threshold is `global_limit / actor_count / ENTRY_MEM_BYTES`,
//! derived live from [`crate::mem_budget::GlobalMemBudget`] on every insert. As new
//! clients spawn more actors the threshold shrinks automatically, so the sum of
//! all in-memory buffers never exceeds the hard ceiling. Under elevated pressure
//! actors flush preemptively (Behavior B) so the ceiling is rarely hit at all.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use indexmap::IndexMap;
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use rust_decimal::Decimal;
use tempfile::{Builder, NamedTempFile};

use crate::env::resolve_ledger_dir;
use crate::errors::EngineError;
use crate::mem_budget::{GlobalMemBudget, MemPressure};
use crate::models::{Account, TransactionInfo, TransactionType};
// ---------------------------------------------------------------------------
// On-disk table
// ---------------------------------------------------------------------------

/// The deposit ledger table stored in redb.
///
/// * Key   — `u32` global transaction ID
/// * Value — 18-byte [`DepositRecord`] encoding:
///   `[0..2]` = client (u16, little-endian) | `[2..18]` = amount (Decimal
///   16-byte serialisation via [`rust_decimal::Decimal::serialize`])
const DEPOSITS: TableDefinition<u32, [u8; 18]> = TableDefinition::new("deposits");

/// Encode a [`DepositRecord`] into 18 bytes for redb storage.
fn encode_record(r: &DepositRecord) -> [u8; 18] {
    let mut buf = [0u8; 18];
    buf[..2].copy_from_slice(&r.client.to_le_bytes());
    buf[2..].copy_from_slice(&r.amount.serialize());
    buf
}

/// Decode a 18-byte redb value back into a [`DepositRecord`].
fn decode_record(buf: [u8; 18]) -> DepositRecord {
    let client = u16::from_le_bytes([buf[0], buf[1]]);
    let amount = Decimal::deserialize([
        buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
        buf[13], buf[14], buf[15], buf[16], buf[17],
    ]);
    DepositRecord { client, amount }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// The two legal states of a client account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountState {
    Active,
    Locked,
}

/// All the data needed to later process a dispute against a deposit.
///
/// Whether a deposit is currently under dispute is tracked exclusively by
/// [`Payments::disputed`] to avoid a disk write on every state transition.
#[derive(Debug, Clone)]
struct DepositRecord {
    client: u16,
    amount: Decimal,
}

/// Approximate memory cost per pending-buffer entry: tx-ID key (`u32`) plus
/// deposit-record value (`DepositRecord`). Does not include `IndexMap`'s
/// internal hash-table overhead (~4–8 bytes per slot on 64-bit), so effective
/// peak RSS will be modestly higher. The formula gives operators a simple,
/// predictable mental model when sizing [`crate::env::TX_MEMORY_ENV`].
const ENTRY_MEM_BYTES: usize = std::mem::size_of::<u32>() + std::mem::size_of::<DepositRecord>();

/// Per-account state machine.
///
/// Encapsulates balance fields and the [`AccountState`] transition logic.
/// Methods return `Err` only for programming errors (e.g. an amount-less
/// deposit reaches this layer). Business-rule rejections (insufficient funds,
/// locked account, unknown tx) are handled silently — they are not fatal to
/// the engine.
#[derive(Debug, Clone)]
struct AccountFsm {
    client: u16,
    available: Decimal,
    held: Decimal,
    state: AccountState,
}

impl AccountFsm {
    fn new(client: u16) -> Self {
        Self {
            client,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            state: AccountState::Active,
        }
    }

    fn is_locked(&self) -> bool {
        self.state == AccountState::Locked
    }

    fn total(&self) -> Decimal {
        self.available + self.held
    }

    // -- Transitions ---------------------------------------------------------

    /// Credit `amount` to available. No-op if account is locked.
    fn apply_deposit(&mut self, amount: Decimal) {
        if self.is_locked() {
            return;
        }
        self.available += amount;
    }

    /// Debit `amount` from available. No-op if locked or insufficient funds.
    fn apply_withdrawal(&mut self, amount: Decimal) {
        if self.is_locked() {
            return;
        }
        if self.available < amount {
            // Insufficient funds, which means: total amount of funds should not change
            return;
        }
        self.available -= amount;
    }

    /// Move `amount` from available → held. Returns `false` (without mutating
    /// state) if the account is locked or available funds are insufficient.
    ///
    /// Called when a dispute is opened on a deposit that belongs to this account.
    fn apply_dispute(&mut self, amount: Decimal) -> bool {
        if self.is_locked() {
            return false;
        }
        if self.available < amount {
            return false;
        }
        self.available -= amount;
        self.held += amount;
        true
    }

    /// Move `amount` from held → available. No-op if locked.
    ///
    /// Called when a dispute is resolved (client drops the claim).
    fn apply_resolve(&mut self, amount: Decimal) {
        if self.is_locked() {
            return;
        }
        let to_release = amount.min(self.held);
        self.held -= to_release;
        self.available += to_release;
    }

    /// Deduct `amount` from held and **lock** the account.
    ///
    /// Called when a chargeback is finalised.
    fn apply_chargeback(&mut self, amount: Decimal) {
        if self.is_locked() {
            return;
        }
        let to_deduct = amount.min(self.held);
        self.held -= to_deduct;
        // Transition: Active → Locked (terminal state)
        self.state = AccountState::Locked;
    }

    // -- Projection ----------------------------------------------------------

    /// Project internal state into the serialisable [`Account`] model.
    fn to_account(&self) -> Account {
        Account {
            client: self.client,
            available: self.available,
            held: self.held,
            total: self.total(),
            locked: self.is_locked(),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Lazily-created redb backing store for the deposit ledger.
///
/// Created on the first call to [`Payments::flush_oldest`]. Field declaration
/// order is load-bearing: `db` must be dropped before `_tmp` so the database
/// handle is closed before its backing file is deleted.
struct Spill {
    db: Database,
    _tmp: NamedTempFile,
}

/// The payment engine.
///
/// Holds all derived state produced by replaying the event stream.
/// Construct with [`Payments::new`], feed events with [`Payments::apply`],
/// then call [`Payments::accounts`] to obtain the final account snapshots.
///
/// The engine is intentionally synchronous. The parallel driver in
/// `crate::parallel` spawns one `Payments` instance per client inside a
/// tokio task, routing transactions by `client_id` so each instance only
/// ever sees a single client's events.
pub struct Payments {
    /// Derived account state, keyed by client ID.
    accounts: HashMap<u16, AccountFsm>,

    /// Set of tx IDs that are currently under an open dispute.
    ///
    /// Kept in memory for O(1) membership tests without a disk lookup.
    disputed: HashSet<u32>,

    /// Deposits buffered in memory, ordered by arrival time.
    ///
    /// [`IndexMap`] preserves insertion order, which lets `flush_oldest` evict
    /// the earliest entries first. Capped at `max_pending`; when that threshold
    /// is reached the oldest 10 % are drained to the redb spill while the
    /// remaining 90 % stay in memory.
    pending: IndexMap<u32, DepositRecord>,

    /// Lazily-created redb spill. `None` until the first buffer overflow.
    spill: Option<Spill>,

    /// Shared aggregate memory tracker. See [`crate::mem_budget`].
    ///
    /// Every pending-buffer insertion reserves [`ENTRY_MEM_BYTES`] from this
    /// budget; every flush releases the bytes it drained. The `Arc` is cloned
    /// by `crate::parallel` when spawning each per-client actor so every
    /// actor contends on the same counter.
    budget: Arc<GlobalMemBudget>,
}

impl Payments {
    /// Create a new, empty engine bound to the shared aggregate memory budget.
    ///
    /// Reads [`crate::env::TX_MEMORY_ENV`] to determine the per-actor
    /// pending-buffer ceiling. The redb spill file is **not** created here;
    /// it is opened lazily on the first buffer overflow.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::EngineInitError`] if [`crate::env::TX_MEMORY_ENV`]
    /// is set but contains an unparseable value.
    pub fn new(budget: Arc<GlobalMemBudget>) -> Result<Self, EngineError> {
        budget.register_actor();
        Ok(Self {
            accounts: HashMap::new(),
            disputed: HashSet::new(),
            pending: IndexMap::new(),
            spill: None,
            budget,
        })
    }

    /// Apply a single event to the engine state.
    ///
    /// Business-rule violations (unknown tx, locked account, insufficient
    /// funds, etc.) are silently skipped — they are not errors in the
    /// engine's operation. Only structural problems (e.g. a deposit missing
    /// its `amount` field) or I/O failures against the ledger return `Err`.
    pub fn apply(&mut self, event: TransactionInfo) -> Result<()> {
        match event.type_tx {
            TransactionType::Deposit => self.handle_deposit(event)?,
            TransactionType::Withdrawal => self.handle_withdrawal(event)?,
            TransactionType::Dispute => self.handle_dispute(event)?,
            TransactionType::Resolve => self.handle_resolve(event)?,
            TransactionType::Chargeback => self.handle_chargeback(event)?,
        }
        Ok(())
    }

    /// Consume the engine and return the final [`Account`] snapshot for every
    /// client seen during processing.
    ///
    /// Row ordering is unspecified (matches the spec: "row ordering does not matter").
    pub fn accounts(mut self) -> impl Iterator<Item = Account> {
        // `mem::take` leaves the field Default-initialised so the engine stays
        // whole and its `Drop` impl (which releases the pending-buffer bytes
        // back to the shared budget) can still run when `self` goes out of
        // scope — moving a field out directly triggers E0509.
        std::mem::take(&mut self.accounts)
            .into_values()
            .map(|fsm| fsm.to_account())
    }

    // -- Event handlers ------------------------------------------------------

    fn handle_deposit(&mut self, event: TransactionInfo) -> Result<()> {
        let amount = require_amount(&event)?;

        // Duplicate check: pending buffer first (cheap), then the on-disk
        // ledger. Tx IDs are globally unique per spec, so a hit here means
        // the partner re-sent the same event and we silently skip it.
        if self.pending.contains_key(&event.tx) || self.db_contains(event.tx)? {
            eprintln!(
                "duplicate tx {} ignored: already exists in pending buffer or on-disk ledger",
                event.tx
            );
            return Ok(());
        }

        // Reserve memory for this new entry. If we'd cross the global
        // ceiling, flush once to free space and retry. If reservation still
        // fails, other actors are holding all the memory and we have nothing
        // left to flush locally — force the reservation so counters stay
        // consistent and log, rather than drop a valid transaction.
        if !self.budget.try_reserve(ENTRY_MEM_BYTES) {
            self.flush_oldest()?;
            if !self.budget.try_reserve(ENTRY_MEM_BYTES) {
                self.budget.force_reserve(ENTRY_MEM_BYTES);
                eprintln!(
                    "tx {}: global memory budget exhausted; proceeding over limit",
                    event.tx
                );
            }
        }

        self.pending.insert(
            event.tx,
            DepositRecord {
                client: event.client,
                amount,
            },
        );
        self.account_mut(event.client).apply_deposit(amount);

        // Behavior B — adaptive post-insert flush. Under rising global
        // pressure the effective threshold shrinks so every actor flushes
        // earlier and contributes to freeing memory before the hard ceiling
        // is hit.
        if self.pending.len() >= self.effective_flush_threshold() {
            self.flush_oldest()?;
        }

        Ok(())
    }

    /// Post-insert flush threshold, in number of pending entries.
    ///
    /// The cap is `budget.suggested_per_actor_cap(ENTRY_MEM_BYTES)` — the
    /// global limit divided fairly across all live actors — so the proactive
    /// flush always fires at a level consistent with the global budget,
    /// adapting automatically as the actor pool grows or shrinks.
    ///
    /// | Pressure | Threshold      | Rationale                                               |
    /// |----------|----------------|---------------------------------------------------------|
    /// | Low      | `per_actor`    | Normal operation.                                       |
    /// | Medium   | `per_actor / 2`| Spread I/O earlier; buy headroom before the ceiling.    |
    /// | High     | `usize::MAX`   | Pre-insert reservation retry already flushed; skip the  |
    /// |          |                | post-insert check to avoid a redundant second flush.    |
    fn effective_flush_threshold(&self) -> usize {
        let per_actor = self.budget.suggested_per_actor_cap(ENTRY_MEM_BYTES);
        match self.budget.pressure() {
            MemPressure::Low => per_actor,
            MemPressure::Medium => (per_actor / 2).max(1),
            MemPressure::High => usize::MAX,
        }
    }

    fn handle_withdrawal(&mut self, event: TransactionInfo) -> Result<()> {
        let amount = require_amount(&event)?;
        self.account_mut(event.client).apply_withdrawal(amount);
        Ok(())
    }

    fn handle_dispute(&mut self, event: TransactionInfo) -> Result<()> {
        // Lookup the referenced deposit; ignore if not found (partner error).
        let record = match self.lookup_deposit(event.tx)? {
            Some(r) => r,
            None => return Ok(()),
        };

        // Cross-client dispute guard: only the owning client may dispute.
        if record.client != event.client {
            return Ok(());
        }

        // Ignore if already disputed (idempotency guard).
        if self.disputed.contains(&event.tx) {
            return Ok(());
        }

        let amount = record.amount;
        self.disputed.insert(event.tx);

        if !self.account_mut(event.client).apply_dispute(amount) {
            // Undo the disputed flag — the dispute was not applied.
            self.disputed.remove(&event.tx);
            eprintln!(
                "dispute for tx {} (client {}) ignored: insufficient available funds",
                event.tx, event.client
            );
        }

        Ok(())
    }

    fn handle_resolve(&mut self, event: TransactionInfo) -> Result<()> {
        // The tx must exist, belong to this client, and currently be disputed.
        let record = match self.lookup_deposit(event.tx)? {
            Some(r) => r,
            None => return Ok(()),
        };

        if record.client != event.client {
            return Ok(());
        }

        if !self.disputed.contains(&event.tx) {
            return Ok(());
        }

        self.disputed.remove(&event.tx);
        self.account_mut(event.client).apply_resolve(record.amount);

        Ok(())
    }

    fn handle_chargeback(&mut self, event: TransactionInfo) -> Result<()> {
        // The tx must exist, belong to this client, and currently be disputed.
        let record = match self.lookup_deposit(event.tx)? {
            Some(r) => r,
            None => return Ok(()),
        };

        if record.client != event.client {
            return Ok(());
        }

        if !self.disputed.contains(&event.tx) {
            return Ok(());
        }

        // Mark as no longer disputed (it's now finalised / charged back).
        self.disputed.remove(&event.tx);
        self.account_mut(event.client)
            .apply_chargeback(record.amount);

        Ok(())
    }

    // -- Helpers -------------------------------------------------------------

    /// Return a mutable reference to the account FSM for `client`, creating a
    /// new one if it does not yet exist.
    fn account_mut(&mut self, client: u16) -> &mut AccountFsm {
        self.accounts
            .entry(client)
            .or_insert_with(|| AccountFsm::new(client))
    }

    // -- redb helpers --------------------------------------------------------

    /// Look up a deposit by tx ID — pending buffer first, then on-disk spill.
    ///
    /// Returns `Ok(None)` immediately when no spill exists yet, since every
    /// deposit inserted before the first overflow is still in `pending`.
    fn lookup_deposit(&self, tx_id: u32) -> Result<Option<DepositRecord>> {
        if let Some(record) = self.pending.get(&tx_id) {
            return Ok(Some(record.clone()));
        }
        let spill = match &self.spill {
            Some(s) => s,
            None => return Ok(None),
        };
        let txn = spill
            .db
            .begin_read()
            .context("failed to begin read transaction")?;
        let table = txn
            .open_table(DEPOSITS)
            .context("failed to open deposits table")?;
        Ok(table
            .get(tx_id)
            .context("failed to look up deposit record")?
            .map(|guard| decode_record(guard.value())))
    }

    /// Return `true` if `tx_id` is already present in the on-disk spill.
    ///
    /// Returns `false` immediately when no spill exists yet — all deposits are
    /// still in `pending`, which the caller already checked.
    fn db_contains(&self, tx_id: u32) -> Result<bool> {
        let spill = match &self.spill {
            Some(s) => s,
            None => return Ok(false),
        };
        let txn = spill
            .db
            .begin_read()
            .context("failed to begin read transaction")?;
        let table = txn
            .open_table(DEPOSITS)
            .context("failed to open deposits table")?;
        Ok(table
            .get(tx_id)
            .context("failed to look up deposit record")?
            .is_some())
    }

    /// Evict the oldest 10 % of buffered deposits to the redb spill store,
    /// creating it lazily on the first call.
    ///
    /// Because [`IndexMap`] preserves insertion order, draining the front of
    /// the map yields exactly the entries that have been resident longest.
    /// A single redb write transaction covers all evicted records, keeping
    /// per-commit overhead low. After the call, `pending` retains ~90 % of
    /// its previous entries and is ready to accept new deposits.
    fn flush_oldest(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        // Open the redb file the first time the buffer overflows.
        if self.spill.is_none() {
            self.spill = Some(Self::open_spill()?);
        }

        // Evict at least one entry even when pending has fewer than 10 items.
        let flush_count = (self.pending.len() / 10).max(1);

        if let Some(spill) = self.spill.as_mut() {
            let mut txn = spill
                .db
                .begin_write()
                .context("failed to begin write transaction")?;
            txn.set_durability(Durability::None)?;
            {
                let mut table = txn
                    .open_table(DEPOSITS)
                    .context("failed to open deposits table")?;
                // `drain(..flush_count)` removes the first `flush_count` entries in
                // insertion order (the oldest arrivals) and yields them as an
                // iterator — O(n) over the drained slice, no full-map rebuild.
                for (tx_id, record) in self.pending.drain(..flush_count) {
                    table
                        .insert(tx_id, encode_record(&record))
                        .context("failed to insert deposit record")?;
                }
            }
            txn.commit().context("failed to commit deposit batch")?;
        }

        // Return the freed bytes to the shared budget so other actors (or
        // this one on its next insert) can take them.
        self.budget.release(flush_count * ENTRY_MEM_BYTES);

        Ok(())
    }

    /// Create and initialise a fresh redb spill file.
    fn open_spill() -> Result<Spill> {
        let dir = resolve_ledger_dir();
        let tmp = Builder::new()
            .prefix("pecrab-ledger-")
            .tempfile_in(&dir)
            .with_context(|| {
                format!(
                    "failed to create temporary ledger file in {}",
                    dir.display()
                )
            })?;
        let db = Database::create(tmp.path()).context("failed to open redb database")?;
        let mut txn = db
            .begin_write()
            .context("failed to begin initial write transaction")?;
        txn.set_durability(Durability::None)?;
        txn.open_table(DEPOSITS)
            .context("failed to initialise deposits table")?;
        txn.commit()
            .context("failed to commit table initialisation")?;
        Ok(Spill { db, _tmp: tmp })
    }
}

impl Drop for Payments {
    /// Return every byte still held in `pending` to the shared budget and
    /// decrement the live-actor count.
    ///
    /// Single-shot CSV runs drop the budget alongside the engine so the
    /// counter dies with the process either way, but a clean release keeps
    /// accounting honest for long-lived budgets (e.g. future interactive or
    /// streaming deployments where the budget outlives individual actors).
    fn drop(&mut self) {
        let outstanding = self.pending.len().saturating_mul(ENTRY_MEM_BYTES);
        if outstanding > 0 {
            self.budget.release(outstanding);
        }
        self.budget.deregister_actor();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the `amount` field from an event that requires one (deposit,
/// withdrawal). Returns `Err` if the field is absent — that signals a
/// malformed input row, which is a structural error worth surfacing.
fn require_amount(event: &TransactionInfo) -> Result<Decimal> {
    match event.amount {
        Some(a) => Ok(a),
        None => bail!(
            "tx {} (type {:?}) is missing required `amount` field",
            event.tx,
            event.type_tx
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn deposit(client: u16, tx: u32, amount: Decimal) -> TransactionInfo {
        TransactionInfo {
            type_tx: TransactionType::Deposit,
            client,
            tx,
            amount: Some(amount),
        }
    }

    fn withdrawal(client: u16, tx: u32, amount: Decimal) -> TransactionInfo {
        TransactionInfo {
            type_tx: TransactionType::Withdrawal,
            client,
            tx,
            amount: Some(amount),
        }
    }

    fn dispute(client: u16, tx: u32) -> TransactionInfo {
        TransactionInfo {
            type_tx: TransactionType::Dispute,
            client,
            tx,
            amount: None,
        }
    }

    fn resolve(client: u16, tx: u32) -> TransactionInfo {
        TransactionInfo {
            type_tx: TransactionType::Resolve,
            client,
            tx,
            amount: None,
        }
    }

    fn chargeback(client: u16, tx: u32) -> TransactionInfo {
        TransactionInfo {
            type_tx: TransactionType::Chargeback,
            client,
            tx,
            amount: None,
        }
    }

    fn accounts_map(engine: Payments) -> HashMap<u16, Account> {
        engine.accounts().map(|a| (a.client, a)).collect()
    }

    // -- flush_oldest --------------------------------------------------------

    /// Build an engine whose budget allows at most `cap` entries.
    ///
    /// The (`cap` + 1)-th deposit fails `try_reserve` and triggers the
    /// pre-insert flush path.
    fn engine_with_cap(cap: usize) -> Payments {
        let budget = Arc::new(GlobalMemBudget::new(cap * ENTRY_MEM_BYTES));
        Payments::new(budget).unwrap()
    }

    #[test]
    fn flush_oldest_evicts_to_redb_and_deposit_still_disputable() {
        // cap=1: a second deposit exhausts the budget, triggering a pre-insert
        // flush that moves tx 1 to redb before tx 2 is inserted.
        let mut e = engine_with_cap(1);
        e.apply(deposit(1, 1, dec!(100.0))).unwrap(); // fills the budget
        e.apply(deposit(1, 2, dec!(1.0))).unwrap(); // pre-insert flush: tx 1 → redb

        // tx 1 must be findable via the on-disk ledger.
        e.apply(dispute(1, 1)).unwrap();

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].held, dec!(100.0));
    }

    #[test]
    fn flush_oldest_keeps_newer_entries_in_memory() {
        // Use an unlimited budget and call flush_oldest directly to isolate the
        // eviction policy: oldest 10 % are drained, newest stay in memory.
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(10.0))).unwrap();
        e.apply(deposit(1, 2, dec!(20.0))).unwrap();

        e.flush_oldest().unwrap();

        // Oldest entry (tx 1) should be in redb; newest (tx 2) stays in pending.
        assert!(!e.pending.contains_key(&1));
        assert!(e.pending.contains_key(&2));

        // Both must still be disputable.
        e.apply(dispute(1, 1)).unwrap();
        e.apply(dispute(1, 2)).unwrap();

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].held, dec!(30.0));
    }

    // -- Deposit -------------------------------------------------------------

    #[test]
    fn deposit_creates_account_and_credits_available() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(100.0000));
        assert_eq!(acc.held, dec!(0));
        assert_eq!(acc.total, dec!(100.0000));
        assert!(!acc.locked);
    }

    #[test]
    fn duplicate_tx_id_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(deposit(1, 1, dec!(50.0000))).unwrap(); // same tx ID

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
    }

    // -- Withdrawal ----------------------------------------------------------

    #[test]
    fn withdrawal_debits_available() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(withdrawal(1, 2, dec!(40.0000))).unwrap();

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(60.0000));
        assert_eq!(accounts[&1].total, dec!(60.0000));
    }

    #[test]
    fn withdrawal_fails_silently_on_insufficient_funds() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(10.0000))).unwrap();
        e.apply(withdrawal(1, 2, dec!(20.0000))).unwrap(); // exceeds available

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(10.0000)); // unchanged
    }

    // -- Dispute -------------------------------------------------------------

    #[test]
    fn dispute_moves_funds_to_held() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 1)).unwrap();

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(0.0));
        assert_eq!(acc.held, dec!(100.0));
        assert_eq!(acc.total, dec!(100.0));
    }

    #[test]
    fn dispute_with_insufficient_available_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0))).unwrap();
        e.apply(withdrawal(1, 2, dec!(100.0))).unwrap(); // drain available
        e.apply(dispute(1, 1)).unwrap(); // available < deposit amount → ignored

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(0.0));
        assert_eq!(acc.held, dec!(0.0));
        assert!(!acc.locked);
    }

    #[test]
    fn dispute_unknown_tx_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 99)).unwrap(); // tx 99 does not exist

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    #[test]
    fn dispute_cross_client_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(2, 1)).unwrap(); // client 2 tries to dispute client 1's tx

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    #[test]
    fn dispute_idempotency() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 1)).unwrap();
        e.apply(dispute(1, 1)).unwrap(); // duplicate dispute

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(0.0000));
        assert_eq!(acc.held, dec!(100.0000));
    }

    // -- Resolve -------------------------------------------------------------

    #[test]
    fn resolve_moves_held_back_to_available() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 1)).unwrap();
        e.apply(resolve(1, 1)).unwrap();

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(100.0000));
        assert_eq!(acc.held, dec!(0.0000));
        assert_eq!(acc.total, dec!(100.0000));
    }

    #[test]
    fn resolve_on_undisputed_tx_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(resolve(1, 1)).unwrap(); // not disputed

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    // -- Chargeback ----------------------------------------------------------

    #[test]
    fn chargeback_deducts_held_and_locks_account() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 1)).unwrap();
        e.apply(chargeback(1, 1)).unwrap();

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.available, dec!(0.0000));
        assert_eq!(acc.held, dec!(0.0000));
        assert_eq!(acc.total, dec!(0.0000));
        assert!(acc.locked);
    }

    #[test]
    fn chargeback_on_undisputed_tx_is_ignored() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(chargeback(1, 1)).unwrap(); // no prior dispute

        let accounts = accounts_map(e);
        assert!(!accounts[&1].locked);
        assert_eq!(accounts[&1].total, dec!(100.0000));
    }

    #[test]
    fn locked_account_ignores_all_mutations() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 1)).unwrap();
        e.apply(chargeback(1, 1)).unwrap(); // locks the account

        e.apply(deposit(1, 2, dec!(500.0000))).unwrap(); // should be ignored
        e.apply(withdrawal(1, 3, dec!(10.0000))).unwrap(); // should be ignored

        let accounts = accounts_map(e);
        let acc = &accounts[&1];
        assert_eq!(acc.total, dec!(0.0000));
        assert!(acc.locked);
    }

    // -- Multi-client --------------------------------------------------------

    #[test]
    fn multiple_clients_are_independent() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(deposit(2, 2, dec!(200.0000))).unwrap();
        e.apply(withdrawal(1, 3, dec!(50.0000))).unwrap();

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(50.0000));
        assert_eq!(accounts[&2].available, dec!(200.0000));
    }

    // -- require_amount ------------------------------------------------------

    #[test]
    fn deposit_without_amount_returns_err() {
        let mut e = Payments::new(Arc::new(GlobalMemBudget::unlimited())).unwrap();
        let result = e.apply(TransactionInfo {
            type_tx: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: None,
        });
        assert!(result.is_err());
    }

    // -- Global memory budget -----------------------------------------------

    #[test]
    fn budget_tracks_bytes_held_in_pending() {
        let budget = Arc::new(GlobalMemBudget::unlimited());
        let mut e = Payments::new(budget.clone()).unwrap();
        e.apply(deposit(1, 1, dec!(1.0))).unwrap();
        e.apply(deposit(1, 2, dec!(2.0))).unwrap();
        assert_eq!(budget.in_use(), ENTRY_MEM_BYTES * 2);
    }

    #[test]
    fn budget_released_when_engine_dropped() {
        let budget = Arc::new(GlobalMemBudget::unlimited());
        {
            let mut e = Payments::new(budget.clone()).unwrap();
            e.apply(deposit(1, 1, dec!(1.0))).unwrap();
            e.apply(deposit(1, 2, dec!(2.0))).unwrap();
            assert_eq!(budget.in_use(), ENTRY_MEM_BYTES * 2);
        } // engine dropped here
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn budget_released_on_flush() {
        let budget = Arc::new(GlobalMemBudget::unlimited());
        let mut e = Payments::new(budget.clone()).unwrap();
        for tx in 1..=5u32 {
            e.apply(deposit(1, tx, dec!(1.0))).unwrap();
        }
        let before = budget.in_use();
        e.flush_oldest().unwrap();
        assert!(
            budget.in_use() < before,
            "flush must release bytes to the budget"
        );
    }

    #[test]
    fn high_pressure_forces_flush_before_each_insert() {
        // Budget capped at exactly 2 entries' worth — the third insert is
        // forced to flush before it can reserve.
        let budget = Arc::new(GlobalMemBudget::new(ENTRY_MEM_BYTES * 2));
        let mut e = Payments::new(budget.clone()).unwrap();

        e.apply(deposit(1, 1, dec!(1.0))).unwrap();
        e.apply(deposit(1, 2, dec!(2.0))).unwrap();
        // Budget is now saturated; next insert must spill.
        e.apply(deposit(1, 3, dec!(3.0))).unwrap();

        assert!(e.spill.is_some(), "spill file must have been created");
        assert!(
            budget.in_use() <= ENTRY_MEM_BYTES * 2,
            "budget must stay within its hard ceiling under High pressure",
        );
    }

    #[test]
    fn effective_flush_threshold_adapts_to_pressure() {
        // Budget holds 10 entries total; 1 actor → suggested_per_actor_cap = 10.
        let budget = Arc::new(GlobalMemBudget::new(ENTRY_MEM_BYTES * 10));
        let e = Payments::new(budget.clone()).unwrap();

        // Low pressure: threshold = per_actor_cap = 10.
        assert_eq!(budget.pressure(), MemPressure::Low);
        assert_eq!(e.effective_flush_threshold(), 10);

        // Medium pressure (≥ 80 % of hard limit): threshold halves to 5.
        budget.force_reserve(ENTRY_MEM_BYTES * 8);
        assert_eq!(budget.pressure(), MemPressure::Medium);
        assert_eq!(e.effective_flush_threshold(), 5);

        // High pressure: post-insert check is skipped (pre-insert retry handles it).
        budget.force_reserve(ENTRY_MEM_BYTES * 2);
        assert_eq!(budget.pressure(), MemPressure::High);
        assert_eq!(e.effective_flush_threshold(), usize::MAX);

        // Restore the counter so the engine's Drop does not underflow.
        budget.release(ENTRY_MEM_BYTES * 10);
    }

    #[test]
    fn effective_flush_threshold_shrinks_as_actors_join() {
        // Budget holds 100 entries.
        let budget = Arc::new(GlobalMemBudget::new(ENTRY_MEM_BYTES * 100));

        let e1 = Payments::new(budget.clone()).unwrap(); // actor_count = 1, cap = 100
        assert_eq!(e1.effective_flush_threshold(), 100);

        let e2 = Payments::new(budget.clone()).unwrap(); // actor_count = 2, cap = 50 each
        assert_eq!(e2.effective_flush_threshold(), 50);
        // e1's threshold also shrinks — cap is recomputed live, not cached.
        assert_eq!(e1.effective_flush_threshold(), 50);

        drop(e2); // actor_count → 1; cap returns to 100
        assert_eq!(e1.effective_flush_threshold(), 100);
    }
}
