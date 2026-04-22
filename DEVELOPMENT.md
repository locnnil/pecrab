# Development notes for the project

This document contains notes and ideas for the development of the project.
It is a living document that will be updated as the project progresses.

## Approach decisions

- **Dispute scope**: The disputes only will be valid if the referenced tx belongs to the same client making the dispute.
- **What is disputable**: Only deposits are disputable, withdrawals are assumed to not be disputable.
  - There is no such thing as a dispute for a dispute, or a dispute for a chargeback, or a dispute for a resolution.
- Applying disputes: When a dispute is applied, the amount of the disputed transaction will be moved from available to held, and the total will remain the same.
  - Client with non sufficient available funds to cover the dispute will not be allowed to be disputed, the dispute will be ignored.
- **Duplicate transactions**: If a transaction with the same tx id is encountered, it will be ignored and a warning will be logged

## Generating samples for test cases

For the tests, I've started to draft a sample tests using AI to generate the test cases.
The test data `sample_01_100k_transactions.csv` with 100k transactions was generated using the command:

```bash
#!/bin/bash
# Input file generation
echo 'type,client,tx,amount' > sample_01_100k_transactions.csv;
for i in {1..100001}; do echo "deposit,1,$i,$i.0000" >> sample_01_100k_transactions.csv; done

# Output file generation
echo 'client,available,held,total,locked' > sample_01_out.csv;
echo "1,$(sum=0; for i in {1..100001}; do ((sum += i)); done; echo "$sum").0000,0.0000,$(sum=0; for i in {1..100001}; do ((sum += i)); done; echo "$sum").0000,false" >> sample_13_out.csv
```

> [!WARNING]
> Brace expansion (e.g., `{1..N}`) is evaluated before execution and
> generates the full sequence in memory. Large ranges can cause high
> memory usage or hit shell limits.
>
> For example, `{1..100000000}` expands to a massive argument list,
> which may degrade performance or fail entirely.

### Pushing to the limits

To test the maximum amount of transactions that the application can handle, we can generate a large file with 4.294.967.295 transactions doing this:
```bash
{ echo "type,client,tx,amount"; seq 1 4294967294 | awk '{print "deposit,1,"$0",1.0000"}'; } > sample_00_max_amount_of_transactions.csv
```

> [!NOTE]
> The above command is safe to run as it streams the output directly to the file without loading it all into memory at once.
> However, be aware that generating such a large file will take a significant amount of time and disk space.

To generate the expected output file:
```bash
echo 'client,available,held,total,locked' > sample_00_out.csv;
echo "1,4294967294.0000,0.0000,4294967294.0000,false" >> sample_00_out.csv
```

This will create a file with 4.294.967.295 transactions, which is the maximum value for a u32, and will test the application's ability to handle such a large number of transactions.
The generated file has **111GB** of size, and wasn't included in the repository, but it can be generated using the above command.

```console
 du -sh sample_15_max_amount_of_transactions.csv
111G    sample_15_max_amount_of_transactions.csv
```

The command below executes the application inside a transient systemd scope with a 4 GiB memory cap and an 80% CPU quota.
This isolates resource usage from the rest of the system and allows the run to be monitored via `systemd-cgtop` or `systemctl status`.

```bash
cargo build --release
sudo systemd-run --scope \
    -p MemoryMax=4G \
    -p CPUQuota=80% \
    /usr/bin/time -v \
    ./target/release/pe_crab tests/sample_99_max_amount_of_transactions.csv \
    > sample_99_out.csv
```

So far, without running on a systemd scope, the application get's killed by the OOM killer.

#### Introducing samples with large amount of transactions

To test the application ability of handling a large amount of transactions, was generated samples with
1k, 100k, 1M, 10M, 100M, 500M and 1B transactions.

By executing:
```bash
# Generate 1k transactions
{ echo "type,client,tx,amount"; seq 1 1000 | awk '{print "deposit,1,"$0",1.0"}'; } > sample_99_1k_transactions.csv
echo 'client,available,held,total,locked' > sample_99_out.csv
echo "1,1000.0, 0.0000, 1000.0, false" >> sample_99_out.csv

# Generate 100k transactions
{ echo "type,client,tx,amount"; seq 1 100000 | awk '{print "deposit,1,"$0",1.0"}'; } > sample_98_100k_transactions.csv
echo 'client,available,held,total,locked' > sample_98_out.csv
echo "1,100000.0, 0.0000, 100000.0, false" >> sample_98_out.csv

# ...
```

The samples up to 1M were added to the repository (with file size of 20MB), but the samples with more than 1M transactions weren't added to avoid the need of using git LFS.

#### Possible solutions to handle a large amount of transactions

Some of the solutions considered to handle such a large amount of transactions include:

- **Using a light database like SQLite3**: At first, I thought about using SQLite3 to store the transactions, the raw idea was: Keep in memeory up to a number of transactions, then after this number, flush the transactions to the disk, and when we need to access a transaction, we can read it from the disk. This way we can handle a large number of transactions without consuming too much memory.
  - The problem with this approach is that it would add a lot of complexity to the application, and it would also add a lot of overhead to the performance, since we would need to read and write to the disk for every transaction.
  - Considering also write amplification, since we would need to write the transactions to the disk multiple times, this approach would not be efficient and could cause a 100-1000x performance degradation. Mostly due to the nature of how most dbms handles writes using B-trees.
  - Also we would have to deal with the unsafe C code FFI to use SQLite3, which is not ideal for this project.
  - So, it works, but it's not a good solution for this problem.

- **Parsing the transactions file in multiple passes**: We just need to hold transactions in memory for the case that a dispute is made then it references that transaction.
But if instead switching the mental model to a multi-pass approach, we can first pass the entire file and collect every tx id that is referenced by a dispute.
This would break the buffering model

- **Using an Rust native embedded key-value store like ReDB**: Similar to the SQLite3 approach, but using a Rust native embeded key-value store. With a tempfile backend, we can store the transactions on disk without worrying about the unsafe C code FFI.

The last approach is the one implemented in the current version of the application, and it works well for the test cases (1k, 100k, 1M, 100M and 500M transactions).
The amount of memory used is kept constant around 2GB, due to the constraint in code of keeping up to 50M in memory, and flushing to disk after this number.

#### Parallelization

Since the application is currently single-threaded, it can be parallelized by using multiple threads to process the transactions in parallel.
One of the assumptions is that transactions are isolated by client, so we can process transactions of different clients in parallel without worrying about race conditions.
One possible approach is to use a dispatcher thread that reads the transactions from the file and dispatches them to worker threads based on the client id.
Each worker thread would then process the transactions for its assigned clients and update the accounts accordingly.
