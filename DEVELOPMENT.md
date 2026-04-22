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



