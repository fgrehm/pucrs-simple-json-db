# TODO

- [x] BitMap abstraction
- [x] Buffer with FIFO write back cache and `map[uint16]Datablock` lookup
- [x] Autoincremented ID
- [x] Manually write a string on a datablock referenced by a pointer on the first datablock
- [ ] Basic insertion of strings, 2 records per datablock
- [ ] Binary writer (?)
- [ ] Persist BitMap on the first 2 datablocks
- [ ] Shell with readline enabled
- [ ] Reserve the first 3 datablocks for some internal information (like the datablocks bitmap and the next ID to be used)
- [ ] BTree index
- [ ] Validate JSON provided
- [ ] Infrastructure for integration testing using basht
- [ ] Buffer with Clock cache strategy
- [ ] Buffer with "a more efficient lookup"

# Nice to haves

- [ ] Datablock -> DataBlock, Datafile -> DataFile
- [ ] bitmap.go -> bit_map.go
- [ ] HTTP API
- [ ] Allow configuring datafile and datablock sizes
- [ ] Allow updating records

# Random

- How to flag datablocks that are in use?
  * A bitmap flagging the blocks that are in use seems to be a good idea
  * For 65.536 (2^16) datablocks of 4KB (2^12), a bitmap will eat 2 datablocks (total datablocks / 8 / 1024 = 8 KB)

# Questions

- Read a datablock at a time or set of datablocks in one go?
- Need to deal with PCTFREE / PCTUSED?
- Do we need to have a schema predefined or just accept any JSON data we receive?
