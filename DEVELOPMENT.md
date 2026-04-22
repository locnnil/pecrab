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
{ echo "type,client,tx,amount"; seq 1 4294967294 | awk '{print "deposit,1,"$0",1.0000"}'; } > sample_99_max_amount_of_transactions.csv
```

> [!NOTE]
> The above command is safe to run as it streams the output directly to the file without loading it all into memory at once.
> However, be aware that generating such a large file will take a significant amount of time and disk space.

To generate the expected output file:
```bash
echo 'client,available,held,total,locked' > sample_99_out.csv;
echo "1,4294967294.0000,0.0000,4294967294.0000,false" >> sample_99_out.csv
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
