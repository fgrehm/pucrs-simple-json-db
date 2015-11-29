1. Introduction
2. Datafile, Datablocks and the Buffer
  - What are they and why needed
  - How things work
  - Buffer: FIFO as a start, Clock later
  - Buffer: `map` as a start, "something better" later
  - Recycling memory: https://blog.cloudflare.com/recycling-memory-buffers-in-go/ + benchmarks made
3. CRUD operations and a Shell / CLI
  - Sequential scan on datablocks to retrieve and delete records
  - Pointer for the next available datablock (always move forward, resets when reaches the end)
  - Datablock headers
  - Bitmap for datablocks in use
  - Chained rows
4. BTree index
  - Row ID
  - Datablocks storage
5. Clock caching strategy and search by tag
