// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Payment engine core.
//!
//! # Architecture
//!
//! ## Event sourcing
//! Every [`TransactionInfo`] that arrives is an immutable event. The engine never
//! mutates past events; it only appends to two ledgers:
//!
//! * `tx_ledger` — canonical record of every **deposit** (the only disputable
//!   transaction type). Keyed by the global tx ID.
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

use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use rust_decimal::Decimal;

use crate::models::{Account, TransactionInfo, TransactionType};

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
#[derive(Debug, Clone)]
struct DepositRecord {
    client: u16,
    amount: Decimal,
    /// Whether this deposit is currently under an open dispute.
    disputed: bool,
}

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

/// The payment engine.
///
/// Holds all derived state produced by replaying the event stream.
/// Construct with [`Payments::new`], feed events with [`Payments::apply`],
/// then call [`Payments::accounts`] to obtain the final account snapshots.
///
/// The engine is intentionally synchronous. To support async ingestion (e.g.
/// multiple TCP sockets), wrap event production in `tokio::task::spawn_blocking`
/// or use a channel that feeds into a single-threaded engine loop — the engine
/// itself needs no changes.
pub struct Payments {
    /// Derived account state, keyed by client ID.
    accounts: HashMap<u16, AccountFsm>,

    /// Immutable deposit ledger. Only deposits are stored because they are
    /// the only transaction type that can be disputed per the spec.
    ///
    /// Key: global tx ID.
    tx_ledger: HashMap<u32, DepositRecord>,

    /// Set of tx IDs that are currently under an open dispute.
    ///
    /// Kept separately from `DepositRecord::disputed` for O(1) membership
    /// tests without a ledger lookup.
    disputed: HashSet<u32>,
}

impl Payments {
    /// Create a new, empty engine.
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            tx_ledger: HashMap::new(),
            disputed: HashSet::new(),
        }
    }

    /// Apply a single event to the engine state.
    ///
    /// Business-rule violations (unknown tx, locked account, insufficient
    /// funds, etc.) are silently skipped — they are not errors in the
    /// engine's operation. Only structural problems (e.g. a deposit missing
    /// its `amount` field) return `Err`.
    pub fn apply(&mut self, event: TransactionInfo) -> Result<()> {
        match event.type_tx {
            TransactionType::Deposit => self.handle_deposit(event)?,
            TransactionType::Withdrawal => self.handle_withdrawal(event)?,
            TransactionType::Dispute => self.handle_dispute(event),
            TransactionType::Resolve => self.handle_resolve(event),
            TransactionType::Chargeback => self.handle_chargeback(event),
        }
        Ok(())
    }

    /// Consume the engine and return the final [`Account`] snapshot for every
    /// client seen during processing.
    ///
    /// Row ordering is unspecified (matches the spec: "row ordering does not matter").
    pub fn accounts(self) -> impl Iterator<Item = Account> {
        self.accounts.into_values().map(|fsm| fsm.to_account())
    }

    // -- Event handlers ------------------------------------------------------

    fn handle_deposit(&mut self, event: TransactionInfo) -> Result<()> {
        let amount = require_amount(&event)?;

        // Record the deposit in the immutable ledger before mutating account
        // state. If the tx ID already exists we treat it as a duplicate and
        // skip — tx IDs are globally unique per spec.
        if self.tx_ledger.contains_key(&event.tx) {
            return Ok(());
        }

        self.tx_ledger.insert(
            event.tx,
            DepositRecord {
                client: event.client,
                amount,
                disputed: false,
            },
        );

        self.account_mut(event.client).apply_deposit(amount);
        Ok(())
    }

    fn handle_withdrawal(&mut self, event: TransactionInfo) -> Result<()> {
        let amount = require_amount(&event)?;
        self.account_mut(event.client).apply_withdrawal(amount);
        Ok(())
    }

    fn handle_dispute(&mut self, event: TransactionInfo) {
        // Lookup the referenced deposit; ignore if not found (partner error).
        let record = match self.tx_ledger.get_mut(&event.tx) {
            Some(r) => r,
            None => return,
        };

        // Cross-client dispute guard: only the owning client may dispute.
        if record.client != event.client {
            return;
        }

        // Ignore if already disputed (idempotency guard).
        if record.disputed {
            return;
        }

        let amount = record.amount;
        record.disputed = true;
        self.disputed.insert(event.tx);

        if !self.account_mut(event.client).apply_dispute(amount) {
            // Undo the disputed flag — the dispute was not applied.
            let record = self.tx_ledger.get_mut(&event.tx).expect("just looked up");
            record.disputed = false;
            self.disputed.remove(&event.tx);
            eprintln!(
                "dispute for tx {} (client {}) ignored: insufficient available funds",
                event.tx, event.client
            );
        }
    }

    fn handle_resolve(&mut self, event: TransactionInfo) {
        // The tx must exist, belong to this client, and currently be disputed.
        let record = match self.tx_ledger.get_mut(&event.tx) {
            Some(r) => r,
            None => return,
        };

        if record.client != event.client {
            return;
        }

        if !record.disputed {
            return;
        }

        let amount = record.amount;
        record.disputed = false;
        self.disputed.remove(&event.tx);

        self.account_mut(event.client).apply_resolve(amount);
    }

    fn handle_chargeback(&mut self, event: TransactionInfo) {
        // The tx must exist, belong to this client, and currently be disputed.
        let record = match self.tx_ledger.get_mut(&event.tx) {
            Some(r) => r,
            None => return,
        };

        if record.client != event.client {
            return;
        }

        if !record.disputed {
            return;
        }

        let amount = record.amount;
        // Mark as no longer disputed (it's now finalised / charged back).
        record.disputed = false;
        self.disputed.remove(&event.tx);

        self.account_mut(event.client).apply_chargeback(amount);
    }

    // -- Helpers -------------------------------------------------------------

    /// Return a mutable reference to the account FSM for `client`, creating a
    /// new one if it does not yet exist.
    fn account_mut(&mut self, client: u16) -> &mut AccountFsm {
        self.accounts
            .entry(client)
            .or_insert_with(|| AccountFsm::new(client))
    }
}

impl Default for Payments {
    fn default() -> Self {
        Self::new()
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

    // -- Deposit -------------------------------------------------------------

    #[test]
    fn deposit_creates_account_and_credits_available() {
        let mut e = Payments::new();
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
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(deposit(1, 1, dec!(50.0000))).unwrap(); // same tx ID

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
    }

    // -- Withdrawal ----------------------------------------------------------

    #[test]
    fn withdrawal_debits_available() {
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(withdrawal(1, 2, dec!(40.0000))).unwrap();

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(60.0000));
        assert_eq!(accounts[&1].total, dec!(60.0000));
    }

    #[test]
    fn withdrawal_fails_silently_on_insufficient_funds() {
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(10.0000))).unwrap();
        e.apply(withdrawal(1, 2, dec!(20.0000))).unwrap(); // exceeds available

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(10.0000)); // unchanged
    }

    // -- Dispute -------------------------------------------------------------

    #[test]
    fn dispute_moves_funds_to_held() {
        let mut e = Payments::new();
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
        let mut e = Payments::new();
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
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(1, 99)).unwrap(); // tx 99 does not exist

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    #[test]
    fn dispute_cross_client_is_ignored() {
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(dispute(2, 1)).unwrap(); // client 2 tries to dispute client 1's tx

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    // -- Resolve -------------------------------------------------------------

    #[test]
    fn resolve_moves_held_back_to_available() {
        let mut e = Payments::new();
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
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(resolve(1, 1)).unwrap(); // not disputed

        let accounts = accounts_map(e);
        assert_eq!(accounts[&1].available, dec!(100.0000));
        assert_eq!(accounts[&1].held, dec!(0.0000));
    }

    // -- Chargeback ----------------------------------------------------------

    #[test]
    fn chargeback_deducts_held_and_locks_account() {
        let mut e = Payments::new();
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
        let mut e = Payments::new();
        e.apply(deposit(1, 1, dec!(100.0000))).unwrap();
        e.apply(chargeback(1, 1)).unwrap(); // no prior dispute

        let accounts = accounts_map(e);
        assert!(!accounts[&1].locked);
        assert_eq!(accounts[&1].total, dec!(100.0000));
    }

    #[test]
    fn locked_account_ignores_all_mutations() {
        let mut e = Payments::new();
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
        let mut e = Payments::new();
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
        let mut e = Payments::new();
        let result = e.apply(TransactionInfo {
            type_tx: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: None,
        });
        assert!(result.is_err());
    }
}
