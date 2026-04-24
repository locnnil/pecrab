# Development Notes

This document covers the design decisions made during development, the evolution of the engine
across multiple implementation iterations, and procedures for large-scale stress testing.

## Design Decisions

### Dispute Scope

A dispute is only valid if the referenced transaction (`tx`) belongs to the same client that is
raising the dispute. Cross-client disputes are silently ignored.

### What Is Disputable

Only deposit transactions can be disputed. Withdrawals, chargebacks, and resolutions are not
disputable. There is no concept of a dispute-of-a-dispute.

### Applying Disputes

When a dispute is applied, the amount of the disputed deposit is moved from `available` to
`held`; `total` remains unchanged. If the client's `available` balance is insufficient to cover
the disputed amount, the dispute is ignored.

### Duplicate Transactions

If a transaction with a `tx` ID that has already been processed is encountered, it is ignored
and a warning is emitted to stderr.

---

## Engine Evolution

### Generation 1 — Naive In-Memory (Single-Tier)

The first implementation stored the full deposit history in a single `HashMap<u32,
TransactionInfo>` in memory. This is simple and fast for small inputs, but when transaction
counts approach the u32 limit (~4.3 billion rows) the process is killed by the Linux OOM killer
long before completion.

### Generation 2 — Two-Tier Storage (In-Memory + redb Spill)

To survive large inputs, a second storage tier was introduced: when the in-memory map exceeded a
fixed entry count, the entire map was flushed to a [redb](https://crates.io/crates/redb)
key-value store backed by a temp file on disk. This kept peak memory roughly constant (~2 GB
with a 50 M entry cap) and allowed the engine to process files well beyond 100 M transactions.

The main drawbacks were:
- The hard flush threshold caused a sudden burst of disk I/O.
- Flushing the entire map at once discarded entries that were likely still needed.

### Generation 3 — IndexMap + 1:10 Incremental Spill

The `HashMap` was replaced with an [`IndexMap`](https://crates.io/crates/indexmap), which
preserves insertion order. Instead of flushing everything when the cap was hit, only the oldest
10 % of entries are evicted to redb at a time. This gives the working set a FIFO-like character
— recent deposits stay hot in memory, older ones spill to disk — and spreads the I/O cost across
many small flushes rather than one large one.

### Generation 4 — Parallel Execution with Tokio (Per-Actor redb)

Sequential processing became the bottleneck. Since each client's transactions are independent, a
Tokio-based dispatcher was introduced: a blocking thread reads the CSV and sends each row to a
per-client actor task via an unbounded channel. Each actor owns a `Payments` instance and, if
needed, its own redb instance.

The redb database for each actor is initialised lazily, only when the actor's in-memory cap is
reached; to avoid exhausting file descriptors (`EMFILE`) when thousands of clients are active
simultaneously.

Benchmark results after introducing parallelism:

```
pecrab_engine/sample_01   time: [1.05 ms]   thrpt: [117 KiB/s]
                          change: −96.2 %   thrpt: +2515.9 %

pecrab_engine/sample_05   time: [1.05 ms]   thrpt: [347 KiB/s]
                          change: −95.1 %   thrpt: +1941.3 %
```

### Generation 5 — Global Memory Budget with Soft/Hard Watermarks (Current)

With N actors each holding up to `PECRAB_ACTOR_MEMORY_LIMIT_BYTES` in memory, the aggregate
footprint can reach N × per-actor limit; easily exceeding physical RAM with the full u16 client
space (65 535 clients). The OOM problem reappeared at the actor-pool level.

A shared `GlobalMemBudget` counter (a single `AtomicUsize`) was introduced. Before inserting a
deposit into its local `IndexMap`, every actor reserves the entry's memory cost from the global
budget; on flush it releases the bytes. This keeps the aggregate ceiling bounded regardless of
how many actors are active.

To avoid all actors racing to flush at the same instant when the budget is nearly full, a
three-level pressure model drives adaptive flush thresholds:

| Pressure | Condition                 | Actor response                                    |
|----------|---------------------------|---------------------------------------------------|
| Low      | `used < 80 %` of limit    | Flush only when the per-actor cap is reached.     |
| Medium   | `80 % ≤ used < 100 %`     | Flush preemptively at half per-actor capacity.    |
| High     | `used ≥ 100 %` of limit   | Flush immediately before each insert.             |

The soft watermark (default 80 %) is configurable in basis points. This ramps up pressure
gradually, spreading flush work across actors and keeping peak disk I/O bounded.

---

## Large-Scale Stress Testing

### Generating Maximum-Size Inputs

To generate a file with all ~4.3 billion possible transactions (u32 max), streaming directly to
disk without loading the sequence into memory:

```bash
{ echo "type,client,tx,amount"; seq 1 4294967294 | awk '{print "deposit,1,"$0",1.0000"}'; } \
    > sample_00_max_amount_of_transactions.csv
```

Expected output:

```bash
echo 'client,available,held,total,locked' > sample_00_out.csv
echo "1,4294967294.0000,0.0000,4294967294.0000,false" >> sample_00_out.csv
```

> **Warning:** The resulting file is approximately **111 GB**. It is not included in the
> repository. Generation takes significant time.

### Running Under a Memory and CPU Cap

Use a transient systemd scope to isolate the process and prevent it from impacting the rest of
the system:

```bash
cargo build --release
sudo systemd-run --unit=pecrab \
    --scope \
    -p MemoryMax=4G \
    -p CPUQuota=80% \
    /usr/bin/time -v \
    ./target/release/pe_crab tests/data/sample_00_max_amount_of_transactions.csv \
    > sample_00_out.csv
```

Monitor while running:

```bash
systemd-cgtop /system.slice/pecrab.scope
# or/and
systemctl status pecrab
```

### Multi-Client Stress Samples

The `generate_sample.sh` script produces inputs with multiple clients so that the parallel
engine is exercised realistically. Each client gets `deposits_per_user` deposits of 10.0,
interleaved round-robin and then shuffled:

```bash
# 65 535 clients, 1 000 deposits each → 65 535 000 transactions
./tests/generate_sample.sh 14_max_clients 65535 1000
```

On a reference run this processed 65.5 M transactions in 1 m 26 s ≈ **756 755 tx/s**.

Pre-generated samples included in the repository:

| Sample file                         | Transactions | File size |
|-------------------------------------|--------------|-----------|
| `sample_98_100k_transactions.csv`   | 100 000      | ~2 MB     |
| `sample_97_1M_transactions.csv`     | 1 000 000    | ~20 MB    |
| `sample_96_10M_transactions.csv`    | 10 000 000   | ~200 MB   |
| `sample_14_max_clients.csv`         | 65 535 000   | ~1.3 GB   |
| `sample_16_1B_transactions.csv`     | 1 000 000 000| not in VCS|

> **Warning:** Shell brace expansion (`{1..N}`) generates the full sequence in memory before
> execution. For N > ~10 million use `seq` or `awk` instead, as shown in the commands above.
