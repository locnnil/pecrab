

**prompt**: Change how the pending transactions are processed in memory.
- First, replace the HashMap for pending with a indexmap map.
- Then instead of hardcoding 50M transactions to be keept in memory, fetch an environment variable called PECRAB_TX_MEMORY_MAX with values of memory in SI units, then based on the struct size of a transaction, calculate the amount of transactions that should be kept in memory.
- When the amount of transactions achieve the maximum, don't flush everything, flush 10% of the oldest indexes to redb and leave the rest in memory.
