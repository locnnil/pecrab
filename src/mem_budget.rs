// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Lincoln Wallace

//! Aggregate heap-memory tracker shared by every per-client actor.
//!
//! # Why this exists
//!
//! Each per-client actor owns its own in-memory [`IndexMap`] of pending
//! deposits, sized by [`crate::env::TX_MEMORY_ENV`]. With up to 65 535 clients
//! (u16 maximum), the naive sum of per-actor ceilings dwarfs the physical RAM
//! of any production host — the Linux OOM killer would happily reap the
//! process before anything misbehaves at the application layer.
//!
//! [`GlobalMemBudget`] introduces an aggregate ceiling shared by every actor.
//! Before an actor inserts a deposit into its local map it reserves
//! `ENTRY_MEM_BYTES` from the budget; on flush it releases the bytes it
//! drained. The counter is a single [`AtomicUsize`] and the hot path is two
//! atomic operations, so contention stays bounded even with thousands of
//! actors.
//!
//! # Actor tracking and dynamic per-actor cap
//!
//! The budget tracks the number of live actors via [`GlobalMemBudget::register_actor`]
//! and [`GlobalMemBudget::deregister_actor`]. This count drives
//! [`GlobalMemBudget::suggested_per_actor_cap`], which computes each actor's
//! fair share of the global limit:
//!
//! ```text
//! per_actor_cap = global_limit / actor_count / entry_size
//! ```
//!
//! Actors use this value as their post-insert flush threshold (capped by the
//! explicit `max_pending` upper bound from [`crate::env::TX_MEMORY_ENV`]).
//! Because the cap recomputes on every flush decision it adapts automatically
//! as actors join or leave: a new client spawns a new actor, shrinking every
//! actor's fair share and triggering earlier proactive flushes.
//!
//! # Pressure watermarks
//!
//! The budget exposes a three-level pressure indicator. Actors consult it
//! before each deposit to decide how aggressively to flush:
//!
//! | Pressure                 | Condition                   | Actor response                                  |
//! |--------------------------|-----------------------------|-------------------------------------------------|
//! | [`MemPressure::Low`]     | `used < soft`               | Flush at `min(max_pending, per_actor_cap)`      |
//! | [`MemPressure::Medium`]  | `soft ≤ used < hard`        | Flush preemptively at half the above threshold  |
//! | [`MemPressure::High`]    | `used ≥ hard`               | Pre-insert flush via reservation retry path     |
//!
//! The soft watermark defaults to 80 % of the hard limit. This smooths the
//! cliff-edge behaviour where every actor would otherwise race to spill at
//! exactly the same instant: pressure ramps up, flushes spread across actors,
//! peak I/O stays bounded, and the hard ceiling is rarely reached.
//!
//! [`IndexMap`]: indexmap::IndexMap

use std::sync::atomic::{AtomicUsize, Ordering};

/// Pressure level derived from current utilisation. See the module-level docs
/// for the action each level implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemPressure {
    /// Below the soft watermark. Normal operation.
    Low,
    /// Between soft and hard watermarks. Actors flush preemptively.
    Medium,
    /// At or beyond the hard ceiling. New reservations are refused;
    /// actors must flush before (or alongside) every insert.
    High,
}

/// Default soft watermark, in basis points of the hard limit (80 %).
pub const DEFAULT_SOFT_WATERMARK_BPS: u16 = 8_000;

/// Aggregate memory tracker, shared across all per-client actors.
///
/// Wrap in an [`Arc`](std::sync::Arc) and clone the handle into every actor.
pub struct GlobalMemBudget {
    /// Hard ceiling in bytes. Reservations that would cross this line fail.
    global_limit: usize,
    /// Current reserved bytes across all actors.
    in_use: AtomicUsize,
    /// Soft watermark expressed in basis points of `global_limit` (0–10 000).
    soft_watermark_bps: u16,
    /// Number of live actors currently sharing this budget.
    ///
    /// Incremented by [`register_actor`](Self::register_actor) when an actor
    /// starts and decremented by [`deregister_actor`](Self::deregister_actor)
    /// when it drops. Used by [`suggested_per_actor_cap`](Self::suggested_per_actor_cap)
    /// to compute each actor's fair share of the global limit.
    actor_count: AtomicUsize,
}

impl GlobalMemBudget {
    /// Create a new budget with the default soft watermark
    /// ([`DEFAULT_SOFT_WATERMARK_BPS`] = 80 %).
    pub fn new(global_limit: usize) -> Self {
        Self {
            global_limit,
            in_use: AtomicUsize::new(0),
            soft_watermark_bps: DEFAULT_SOFT_WATERMARK_BPS,
            actor_count: AtomicUsize::new(0),
        }
    }

    /// Override the soft watermark (basis points, clamped to 0–10 000).
    pub fn with_soft_watermark_bps(mut self, bps: u16) -> Self {
        self.soft_watermark_bps = bps.min(10_000);
        self
    }

    /// An effectively unlimited budget — intended for tests and benchmarks
    /// where the aggregate ceiling is not being exercised.
    pub fn unlimited() -> Self {
        Self::new(usize::MAX)
    }

    /// The hard ceiling in bytes.
    pub fn global_limit(&self) -> usize {
        self.global_limit
    }

    /// Current bytes reserved across all actors. Relaxed load — diagnostics
    /// and pressure decisions only, never correctness.
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Number of live actors currently registered with this budget.
    ///
    /// Relaxed load — used only for advisory cap calculations, not for
    /// correctness invariants.
    pub fn actor_count(&self) -> usize {
        self.actor_count.load(Ordering::Relaxed).max(1)
    }

    /// Signal that a new actor has started and is now sharing this budget.
    ///
    /// Must be paired with a matching [`deregister_actor`](Self::deregister_actor)
    /// call when the actor is dropped, typically from the actor's `Drop` impl.
    pub fn register_actor(&self) {
        self.actor_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Signal that an actor has been dropped and is no longer using this budget.
    pub fn deregister_actor(&self) {
        self.actor_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Suggested per-actor pending-buffer cap, in number of entries.
    ///
    /// Divides the global limit evenly across all registered actors so that the
    /// sum of per-actor in-memory buffers never exceeds the hard ceiling when
    /// each actor respects this cap. Returns at least 1.
    ///
    /// Because `actor_count` is updated as new clients are encountered, the
    /// returned value shrinks automatically as the actor pool grows — each
    /// actor's post-insert flush threshold adapts without any manual tuning.
    pub fn suggested_per_actor_cap(&self, entry_size: usize) -> usize {
        let count = self.actor_count();
        (self.global_limit / entry_size.max(1) / count).max(1)
    }

    /// Attempt to reserve `bytes`. Returns `true` when the reservation stays
    /// within `global_limit`, in which case the caller owes a matching
    /// [`release`](Self::release). Returns `false` when the reservation would
    /// cross the ceiling; in that case the counter is rolled back so no bytes
    /// are consumed.
    pub fn try_reserve(&self, bytes: usize) -> bool {
        let prev = self.in_use.fetch_add(bytes, Ordering::AcqRel);
        if prev.saturating_add(bytes) <= self.global_limit {
            true
        } else {
            self.in_use.fetch_sub(bytes, Ordering::AcqRel);
            false
        }
    }

    /// Reserve `bytes` unconditionally, even if the budget is already at or
    /// past the ceiling. Used as a last-resort fallback so bookkeeping stays
    /// consistent when an actor cannot flush any more locally (its `pending`
    /// is already empty) but other actors still hold all the memory.
    ///
    /// Every call still owes a matching [`release`](Self::release).
    pub fn force_reserve(&self, bytes: usize) {
        self.in_use.fetch_add(bytes, Ordering::AcqRel);
    }

    /// Release `bytes` previously reserved via [`try_reserve`](Self::try_reserve)
    /// or [`force_reserve`](Self::force_reserve).
    pub fn release(&self, bytes: usize) {
        self.in_use.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Current pressure level. Actors use this to decide how aggressively to
    /// flush on each insert (Behavior B).
    pub fn pressure(&self) -> MemPressure {
        let used = self.in_use();
        // u128 widening prevents overflow when `global_limit` is close to
        // `usize::MAX` (e.g. an "unlimited" budget).
        let soft = ((self.global_limit as u128 * self.soft_watermark_bps as u128) / 10_000)
            .min(usize::MAX as u128) as usize;

        if used >= self.global_limit {
            MemPressure::High
        } else if used >= soft {
            MemPressure::Medium
        } else {
            MemPressure::Low
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_reserve_within_limit_succeeds() {
        let b = GlobalMemBudget::new(1_000);
        assert!(b.try_reserve(400));
        assert!(b.try_reserve(600));
        assert_eq!(b.in_use(), 1_000);
    }

    #[test]
    fn try_reserve_over_limit_rolls_back() {
        let b = GlobalMemBudget::new(1_000);
        assert!(b.try_reserve(800));
        assert!(!b.try_reserve(300));
        assert_eq!(b.in_use(), 800, "failed reservation must roll back");
    }

    #[test]
    fn release_returns_bytes() {
        let b = GlobalMemBudget::new(1_000);
        assert!(b.try_reserve(800));
        b.release(500);
        assert_eq!(b.in_use(), 300);
        assert!(b.try_reserve(700));
    }

    #[test]
    fn force_reserve_can_exceed_limit() {
        let b = GlobalMemBudget::new(100);
        b.force_reserve(250);
        assert_eq!(b.in_use(), 250);
        assert_eq!(b.pressure(), MemPressure::High);
    }

    #[test]
    fn pressure_low_below_soft_watermark() {
        let b = GlobalMemBudget::new(1_000); // soft = 800
        b.force_reserve(799);
        assert_eq!(b.pressure(), MemPressure::Low);
    }

    #[test]
    fn pressure_medium_between_soft_and_hard() {
        let b = GlobalMemBudget::new(1_000);
        b.force_reserve(850);
        assert_eq!(b.pressure(), MemPressure::Medium);
    }

    #[test]
    fn pressure_high_at_hard_limit() {
        let b = GlobalMemBudget::new(1_000);
        b.force_reserve(1_000);
        assert_eq!(b.pressure(), MemPressure::High);
    }

    #[test]
    fn custom_soft_watermark_bps() {
        let b = GlobalMemBudget::new(1_000).with_soft_watermark_bps(5_000); // 50 %
        b.force_reserve(500);
        assert_eq!(b.pressure(), MemPressure::Medium);
        b.release(1);
        assert_eq!(b.pressure(), MemPressure::Low);
    }

    #[test]
    fn soft_watermark_clamped_to_ten_thousand() {
        let b = GlobalMemBudget::new(1_000).with_soft_watermark_bps(60_000);
        // Clamped to 100 %; soft == hard, so only values ≥ hard land in Medium/High.
        b.force_reserve(999);
        assert_eq!(b.pressure(), MemPressure::Low);
    }

    #[test]
    fn unlimited_never_hits_high_pressure() {
        let b = GlobalMemBudget::unlimited();
        for _ in 0..1_000 {
            assert!(b.try_reserve(1_000_000));
        }
        assert_eq!(b.pressure(), MemPressure::Low);
    }

    // -- Actor tracking -------------------------------------------------------

    #[test]
    fn actor_count_increments_and_decrements() {
        let b = GlobalMemBudget::new(1_000);
        assert_eq!(b.actor_count.load(Ordering::Relaxed), 0);
        b.register_actor();
        b.register_actor();
        assert_eq!(b.actor_count(), 2);
        b.deregister_actor();
        assert_eq!(b.actor_count(), 1);
    }

    #[test]
    fn actor_count_returns_at_least_one_when_zero() {
        let b = GlobalMemBudget::new(1_000);
        // No actors registered; actor_count() must return 1 to avoid division by zero.
        assert_eq!(b.actor_count(), 1);
    }

    #[test]
    fn suggested_per_actor_cap_single_actor() {
        // 1 000 bytes, entry_size = 100 → 10 entries per actor.
        let b = GlobalMemBudget::new(1_000);
        b.register_actor();
        assert_eq!(b.suggested_per_actor_cap(100), 10);
    }

    #[test]
    fn suggested_per_actor_cap_shrinks_with_more_actors() {
        // 1 000 bytes, entry_size = 100 → 10 entries total.
        let b = GlobalMemBudget::new(1_000);
        b.register_actor(); // 1 actor → 10 entries each
        assert_eq!(b.suggested_per_actor_cap(100), 10);
        b.register_actor(); // 2 actors → 5 entries each
        assert_eq!(b.suggested_per_actor_cap(100), 5);
        b.register_actor(); // 3 actors → 3 entries each (integer division)
        assert_eq!(b.suggested_per_actor_cap(100), 3);
    }

    #[test]
    fn suggested_per_actor_cap_is_at_least_one() {
        // Budget too small for even one entry.
        let b = GlobalMemBudget::new(1);
        b.register_actor();
        assert_eq!(b.suggested_per_actor_cap(100), 1);
    }

    #[test]
    fn suggested_per_actor_cap_unlimited_is_large() {
        let b = GlobalMemBudget::unlimited();
        b.register_actor();
        // Should not panic and should return a very large number.
        assert!(b.suggested_per_actor_cap(22) > 1_000_000_000);
    }
}
