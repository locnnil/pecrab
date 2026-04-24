
**prompt**:
The current mechanism of tracking the limit per actor doesn't scale and can blow when the actors approach to the limit of u16 clients.
Implement an atomic global counter for the global memory budget, use an Arc<>, also implement soft/hard watermarks on the redb spill strategy as opose to the current 10% spill strategy. Where the spill to db responds to the pressure of the input stream to smooth the amount of I/Os, on low use < 80% of the limit: flush only when the per

The global memory budget should be get from an environment variable, if not provided, default to 2GB.

Payments.max_pending should be calculated dynamically based on the Global memory budget instead of the current hardcoded.
