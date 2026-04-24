

I need to improve the main README.md file, and also improve and organize the DEVELOPMENT.md file.

Please organize the DEVELOPMENT.md file in a more structured way, if possible, migrate some of the content to the main README.md file, if it is more relevant there.

The README.md file should contain:

- section about the project structure
- section about the main features of the project
  - Talk about the engine has a two tier ephemeral storage system, with a in-memory cache and a disk-based storage layer using redb.
  - The usage of redb as the disk-based storage layer.
    - talk about the environment variable `PECRAB_LEDGER_DIR` that can be set to specify the directory where redb will store the data.
    - The implementation of a soft and hard watermark system to manage the in-memory cache and the spill to the redb storage layer.

- Section about the testing strategy
  - Talk about the [generate_sample.sh](../tests/generate_sample.sh) script and how it can be used to generate test data with different amount of users and deposit transactions per user.
  - Talk about the [benches](../benches) directory and how it contains benchmarks for the main operations of the engine, such as processing transactions and handling disputes.

- Add a section about the development proccess (this can be either on README.md or DEVELOPMENT.md, depending on the amount of content and relevance to the main README.md file)
  - First implementation of the engine: the naive approach, which was a snigle-tier in-memory storage and get's killed by the OOM killer when tx aproaches the u32 limit.
  - Second implementation of the engine: the two-tier approach, which uses an in-memory cache up to a certain limit and then spills to the redb storage layer on disk.
  - Third implementation of the engine: a soft switch to use IndexMap instead of HashMap and change the spill strategy to 1:10 which is: when the in-memory cache reaches 100% of the limit, it spills 10% of the entries to the redb storage layer, instead of spilling all the entries at once. This allows for better performance and less frequent spills to disk.
  - The forth implementation of the engine: The sequential execution of the engine is too slow, so we implemented a parallel execution with Tokio runtime where a dispatch launchs a task actor per user id. And have the limit of transactions calculated per actor. This implementation has 1 DB per actor, and the databases are initialized in a lazy way, where the database for a user is only initialized when it reaches the limit of in-memory cache.
  - The current implementation of the engine: A global memory limit defined by an environment variable, for the in-memory cache was implemented to avoid OOM issues, since the peractor limit x number of actors can still lead to OOM if the number of actors is high. The global memory limit is implemented using a soft and hard watermark system, where the soft watermark triggers a spill to the redb storage layer when the in-memory cache reaches a certain percentage of the limit, and the hard watermark triggers a spill when the in-memory cache reaches 100% of the limit. This allows for better memory management and prevents OOM issues while still maintaining good performance.


