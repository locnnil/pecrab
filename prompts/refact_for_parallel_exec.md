
**prompt**:

Refactor the code for parallel execution:
Refactor the code to enable parallel execution of transactions for different clients.
Core idea: Use tokio runtime, one actor per client, and a dispatcher that routes transactions to the correct actor.
On the first transaction for a client, spawn a tokio task that owns that client's account state and sits reading from a dedicated channel.
The dispatcher looks up or creates the channel for the client and sends the transaction into it.


**Result**: Claude generated a second implementation of the engine that doesn't uses redb, but instead uses the old approach of keeping all transactions in memory, but with the addition of parallelization using tokio and actors.
This implementation solves the problem of parallel execution, but brings back the problem of handling a large amount of transactions, since all transactions are kept in memory.
The memory usage will grow linearly with the number of transactions, and it will eventually hit the OOM killer for larger samples.

--- 

**prompt**:
I still want to use ReDB for the parallel execution implementation.
Implement a two-tier storage system, where we have the "Hot Tier" in meemory for up to 50M transactions, and the "Cold Tier" on disk using ReDB for transactions beyond that.
The "Hot Tier" will be used for the most recent transactions, and the "Cold Tier" will be used for older transactions in case they are needed for a dispute.
USE the current strategy of batch/buffering where at the startup it's read from an env variable the amount of memory that the engine is allowed to use at runtime per actor, and
also keep the strategy of 1:10 regarding the buffering and batch, which means, if the buffering is 256MB, once it is completed, the oldest 25.6MB will be stored into the redb database.
