# TODO

- [x] BitMap abstraction
- [ ] Autoincrement ID for records
- [ ] Shell with readline enabled
- [ ] Reserve the first 4 datablocks for some internal information (like the datablocks bitmap and the next ID to be used)
- [ ] BTree index
- [ ] Infrastructure for integration testing using basht
- [ ] Buffer with FIFO cache strategy and `map[int]Datablock` lookup
- [ ] Buffer with Clock cache strategy
- [ ] Buffer with "a more efficient lookup"

# Nice to haves

- [ ] Allow configuring datafile and datablock sizes

# Random

- How to flag datablocks that are in use?
  * A bitmap flagging the blocks that are in use seems to be a good idea
  * For 65.536 (2^16) datablocks of 4KB (2^12), a bitmap will eat 2 datablocks (total datablocks / 8 / 1024 = 8 KB)

# Questions

- Read a datablock at a time or set of datablocks in one go?
- Need to deal with PCTFREE / PCTUSED?
