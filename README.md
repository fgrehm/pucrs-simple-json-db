# simple-json-db

A rudimentary JSON database, built for the PUCRS Database Implementation 2015.2 course

## Build

First [install Go](http://golang.org/doc/install) and [gb](http://getgb.io/) into
your environment then:

```
git clone https://github.com/fgrehm/pucrs-simple-json-db.git
cd pucrs-simple-json-db
make build
```

Alternatively, if you have [Docker](https://www.docker.com/) installed, just
`make hack` and `make build` from within the container.

## Usage

```
./bin/sjdb-cli
```

## Packages

  - `bplustree`: Main logic for manipulating B+ Trees. It is used as the foundation
    for B+ Tree DB indexes and it implements the core logic for spliting nodes,
    finding data based on search keys, etc... Users of this package are expected
    to defined and implement the logic for persisting Nodes into the filesystem.
  - `cmd/sjdb-cli`: Console app that connectes to the DB for executing arbitrary commands.
  - `simplejsondb/actions`: High level actions that can be performed against the DB.
  - `simplejsondb/core`: High level abstractings for dealing with reading and writing
    data to / from the filesystem.
  - `simplejsondb/dbio`: Low level abstractions for persisting data into the filesystem.
  - `simplejsondb`: Exposes the object that "glues" everything together.

## Anatomy of a data block that stores records

- Total size: 4KB
- 1 or more contigous chunks of records data
- End the end of the datablock:
  - 2 bytes for utilization (total bytes in use by the data block)
  - 2 bytes for number of records present on block
  - 4 bytes for pointer to previous and next data blocks on the linked list of data blocks of a given type (index or actual data, 2 points each)
  - For each record header:
    - 4 bytes for the record ID (the primary key)
    - 2 bytes for a pointer that indicates where the record starts
    - 2 bytes for a pointer that indicates the record size
    - 4 bytes for next RowID in case of chained rows (2 for Datablock id and 2 for the record offset inside the datablock)

## Anatomy of a data block that stores BTree+ branches

- Total size: 4KB
- Byte 0: uint8 that stores the flag for the node type flag (1 - branch or 2 - leaf)
- Byte 1-2: uint16 that stores total entries on the node
- Byte 3-4: uint16 that stores the parent datablock id
- Byte 5-8: rowid for sibling pointers (1 uint16 for left sibling pointer and another for the right pointer)
- Each entry takes up 6 bytes (4 for the search key and 2 for the next node datablock ID)
- Max amount of entries: (4096 bytes - 8 bytes for total entries and type flag) / 6 =~ 680

## Anatomy of a data block that stores BTree+ leafs

- Total size: 4KB
- Byte 0: uint8 that stores the flag for the node type flag (1 - branch or 2 - leaf)
- Byte 1-2: uint16 that stores total entries on the node
- Byte 3-4: uint16 that stores the parent datablock id
- Byte 5-8: rowid for sibling pointers (1 uint16 for left sibling pointer and another for the right pointer)
- Each entry takes up 8 bytes (4 for the search key and 4 for the row ID)
- Max amount of entries: (4096 bytes - 7 bytes for total entries and type flag) / 8 =~ 510
