# TODO

- [x] BitMap abstraction
- [x] Buffer with FIFO write back cache and `map[uint16]Datablock` lookup
- [x] Autoincremented ID
- [x] Manually write a string on a datablock referenced by a pointer on the first datablock
- [x] Basic insertion of strings on a single datablock
- [x] Double linked list of datablocks and insertion on multiple datablocks
- [x] Remove records
- [x] "Fill in gaps" left by removed records
- [x] Datablocks map using a bitmap under the hood
- [x] Make use of the datablocks map from the allocator object
- [x] "Collapse" record datablock linked lists when it gets empty and reclaim block (mark as free)
- [x] Chained rows
- [x] Update records
- [x] B+Tree index
- [ ] Remove autoincrement logic
- [ ] Integrate BTree with the rest of the code
- [ ] Fix datablocks map
- [ ] Buffer with Clock cache strategy (and revisit buffer needs required for testing)
- [ ] Search by tag using a sequential read of the list of records present on a datablock derived from the BTree+ index
- [ ] Shell with readline enabled
- [ ] Buffer with "a more efficient lookup"
- [ ] Documentation (including "class diagram")

# Nice to haves

- [ ] "Norm" on Delete X remove
- [ ] Rename packages and CLI to sjdb
- [ ] Rename BTreePlus to BPlusTree
- [ ] HTTP API + form to save data
- [ ] Allow configuring datafile and datablock sizes
- [ ] Deal with datafile that has no space left
- [ ] Refactor tests to reduce copy & paste
- [ ] Consistent use of `*Block` and `*BlockID`
- [ ] `type DataBlockIDType uint16`
- [ ] `type RecordIDType uint32`
- [ ] `type DataBlockWrapper { func DataBlockID() DataBlockIDType }`

# Anatomy of a data block that stores records

- Total size: 4KB
- Records data
- End the end of the datablock:
  - 2 bytes for utilization (total bytes in use by the data block)
  - 2 bytes for number of records present on block
  - 4 bytes for pointer to previous and next data blocks on the linked list of data blocks of a given type (index or actual data, 2 points each)
  - For each record header:
    - 4 bytes for the record ID (the primary key)
    - 2 bytes for a pointer that indicates where the record starts
    - 2 bytes for a pointer that indicates the record size
    - 4 bytes for next RowID in case of chained rows (2 for Datablock id and 2 for the record offset inside the datablock)

# Anatomy of a data block that stores BTree+ branches

- Total size: 4KB
- Byte 0: uint8 that stores the flag for the node type flag (1 - branch or 2 - leaf)
- Byte 1-2: uint16 that stores total entries on the node
- Byte 3-4: uint16 that stores the parent datablock id
- Byte 5-8: rowid for sibling pointers (1 uint16 for left sibling pointer and another for the right pointer)
- Each entry takes up 6 bytes (4 for the search key and 2 for the next node datablock ID)
- Max amount of entries: (4096 bytes - 8 bytes for total entries and type flag) / 6 =~ 680

# Anatomy of a data block that stores BTree+ leafs

- Total size: 4KB
- Byte 0: uint8 that stores the flag for the node type flag (1 - branch or 2 - leaf)
- Byte 1-2: uint16 that stores total entries on the node
- Byte 3-4: uint16 that stores the parent datablock id
- Byte 5-8: rowid for sibling pointers (1 uint16 for left sibling pointer and another for the right pointer)
- Each entry takes up 8 bytes (4 for the search key and 4 for the row ID)
- Max amount of entries: (4096 bytes - 7 bytes for total entries and type flag) / 8 =~ 510

# Random

- How to flag datablocks that are in use?
  * A bitmap flagging the blocks that are in use seems to be a good idea
  * For 65.536 (2^16) datablocks of 4KB (2^12), a bitmap will eat 2 datablocks (total datablocks / 8 / 1024 = 8 KB)
- Multiple "tables" (or collections) or a single one?
  * Single collection
- Need to deal with PCTFREE / PCTUSED?
  * NO
- Can reuse the same header after removing? (meaning a new record with a rowid that existed on the past)
  * YES
- Any need to compress data blocks?
  * NO
