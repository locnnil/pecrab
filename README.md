[![tests](https://github.com/locnnil/pecrab/actions/workflows/test.yml/badge.svg)](https://github.com/locnnil/pecrab/actions/workflows/test.yml)

# PECrab — An Oxidized Payment Engine

PECrab is a Rust payment engine that reads a series of transactions from a CSV file, updates
client accounts (handling deposits, withdrawals, disputes, resolutions, and chargebacks), and
writes the final account state to stdout.

```
cargo run --release -- transactions.csv > accounts.csv
```

## Project Structure

```
pecrab/
├── src/                  # Engine source code
│   ├── main.rs           # Entry point: argument parsing, CSV I/O
│   ├── lib.rs            # Public API (run_with_writer_parallel)
│   ├── engine.rs         # Per-client Payments actor (deposit/withdraw/dispute logic)
│   ├── parallel.rs       # Tokio dispatcher: routes CSV rows to per-client actor tasks
│   ├── models.rs         # Transaction and account data types
│   ├── mem_budget.rs     # GlobalMemBudget: shared aggregate memory tracker
│   ├── env.rs            # Environment-variable helpers (limits, paths)
│   └── errors.rs         # Error types
├── benches/
│   └── engine.rs         # Criterion throughput benchmarks
├── tests/
│   ├── integration_test.rs
│   ├── generate_sample.sh        # Script to generate large test datasets
│   └── data/                     # CSV input/output pairs for integration tests
│       ├── sample_01_simple_deposits_and_withdrawals.csv
│       ├── sample_01_out.csv
│       └── ...
├── Cargo.toml
├── README.md
└── DEVELOPMENT.md
```

## Features

### Transaction Types

| Type         | Effect                                                                              |
|--------------|-------------------------------------------------------------------------------------|
| `deposit`    | Credits the client account; increases `available` and `total`.                      |
| `withdrawal` | Debits the account; decreases `available` and `total`. Fails if funds are insufficient. |
| `dispute`    | Moves the disputed deposit amount from `available` to `held`; `total` unchanged.    |
| `resolve`    | Releases held funds back to `available`; `total` unchanged.                         |
| `chargeback` | Removes held funds from `held` and `total`; locks the account permanently.          |

### Parallel Execution

Transactions are routed by `client_id` to dedicated per-client actor tasks running on a Tokio
runtime. Because each client's transactions are independent, actors run concurrently without
shared mutable state. A dispatcher reads the CSV on a blocking thread and fans rows out to the
appropriate actor via an unbounded channel.

### Two-Tier Ephemeral Storage

To handle transaction files that approach the u32 limit (~4.3 billion rows) without exhausting
RAM, the engine uses a two-tier storage model:

1. **In-memory cache** (`IndexMap`) — each per-client actor keeps recently seen deposits in an
   ordered map for O(1) dispute lookups.
2. **Disk-based storage** ([redb](https://crates.io/crates/redb)) — when the in-memory cache
   reaches its limit, the oldest 10 % of entries are evicted to a redb key-value store backed by
   a temporary file on disk. redb is an embedded, pure-Rust store with MVCC semantics and no
   unsafe FFI.

The spill file is created lazily — actors that never exceed their memory budget never touch disk.

#### Configuring the Ledger Directory

By default the redb file is placed in `/var/tmp` (disk-backed on most systems). Set
`PECRAB_LEDGER_DIR` to redirect it to a specific volume, such as a fast NVMe scratch disk:

```bash
PECRAB_LEDGER_DIR=/mnt/nvme/scratch cargo run --release -- transactions.csv > accounts.csv
```

### Memory Budget and Pressure Watermarks

One environment variable controls memory usage:

| Variable                     | Default | Description                                                                   |
|------------------------------|---------|-------------------------------------------------------------------------------|
| `PECRAB_GLOBAL_MEMORY_LIMIT` | `2G`    | Aggregate in-memory budget across all actors. Accepts SI suffixes: `500M`, `4G`, etc. |

Each actor's flush threshold is computed dynamically as
`PECRAB_GLOBAL_MEMORY_LIMIT / active_actor_count / entry_size`, so the sum of all
in-memory buffers stays within the limit regardless of how many clients are active.

The global budget uses a **soft/hard watermark** system to smooth out burst disk I/O:

| Pressure level | Condition                  | Actor response                                  |
|----------------|----------------------------|-------------------------------------------------|
| **Low**        | `used < 80 %` of limit     | Flush only when the per-actor cap is reached.   |
| **Medium**     | `80 % ≤ used < 100 %`      | Flush preemptively at half per-actor capacity.  |
| **High**       | `used ≥ 100 %` of limit    | Flush immediately before each insert.           |

This graduated response prevents all actors from racing to spill to disk at the same instant,
keeping peak I/O bounded and avoiding OOM conditions even with the full u16 client space
(65 535 clients) active simultaneously.

## Testing

### Integration Tests

Integration tests live in `tests/integration_test.rs` and are driven by CSV pairs in
`tests/data/`. Each pair shares a numeric prefix:

```
tests/data/sample_04_dispute_then_resolve.csv   ← input
tests/data/sample_04_out.csv                    ← expected output
```

Run all tests:

```bash
cargo test
```

### Generating Test Data

The [`tests/generate_sample.sh`](tests/generate_sample.sh) script generates matched input/output
CSV pairs with a configurable number of clients and deposits per client. Transactions are
interleaved round-robin across clients and then shuffled to simulate realistic load.

```
Usage: ./tests/generate_sample.sh <name> <num_users> <deposits_per_user>

Arguments:
  name               Sample name with numeric prefix (e.g. "20_big_test").
                     Produces tests/data/sample_20_big_test.csv and tests/data/sample_20_out.csv.
  num_users          Number of client accounts (1–65535).
  deposits_per_user  Deposits per client (> 0).
                     Total tx count (num_users × deposits_per_user) must not exceed 4294967295.
```

Example — generate a sample with 500 clients and 2 000 deposits each (1 000 000 transactions):

```bash
./tests/generate_sample.sh 20_big_test 500 2000
```

> **Note:** The script uses `shuf` to randomise row order, which loads all rows into memory.

### Benchmarks

The `benches/` directory contains [Criterion](https://crates.io/crates/criterion) throughput
benchmarks that measure end-to-end CSV processing on representative samples, including deposits,
withdrawals, disputes, and chargebacks.

```bash
cargo bench
```

Benchmark results are reported as throughput (bytes/s) and wall time per iteration, making it
easy to spot regressions after engine changes.

## Development Notes

See [DEVELOPMENT.md](DEVELOPMENT.md) for the full history of engine design iterations,
large-scale stress-testing procedures, and key design decisions such as dispute scope rules and
the parallelisation approach.
