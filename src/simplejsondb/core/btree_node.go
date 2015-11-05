package core

// We might want to extract the specific node types out to separate files  if
// the code grows too big

import (
	"fmt"

	"simplejsondb/dbio"
)

const (
	BTREE_TYPE_BRANCH = uint8(1)
	BTREE_TYPE_LEAF   = uint8(2)

	BTREE_POS_TYPE           = 0
	BTREE_POS_ENTRIES_COUNT  = BTREE_POS_TYPE + 1
	BTREE_POS_ENTRIES_OFFSET = 9

	BTREE_LEAF_ENTRY_SIZE      = 8
	BTREE_LEAF_OFFSET_KEY      = 0
	BTREE_LEAF_OFFSET_BLOCK_ID = 4
	BTREE_LEAF_OFFSET_LOCAL_ID = 6
)

type BTreeNode interface {
	DataBlockID() uint16
	IsLeaf() bool
}

type bTreeNode struct {
	block *dbio.DataBlock
}

func (n *bTreeNode) DataBlockID() uint16 {
	return n.block.ID
}

func (n *bTreeNode) IsLeaf() bool {
	return n.block.ReadUint8(BTREE_POS_TYPE) == BTREE_TYPE_LEAF
}

type BTreeBranch interface {
	BTreeNode
	Find(searchKey uint32) RowID
}

type bTreeBranch struct {
	*bTreeNode
}

func CreateBTreeBranch(block *dbio.DataBlock) BTreeBranch {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)
	node := &bTreeNode{block}
	return &bTreeBranch{node}
}

func (n *bTreeBranch) Find(searchKey uint32) RowID {
	return RowID{}
}

type BTreeLeaf interface {
	BTreeNode
	Add(searchKey uint32, rowID RowID)
	Remove(searchKey uint32)
	Find(searchKey uint32) RowID
	All() []RowID
}

type bTreeLeaf struct {
	*bTreeNode
}

func CreateBTreeLeaf(block *dbio.DataBlock) BTreeLeaf {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_LEAF)
	node := &bTreeNode{block}
	return &bTreeLeaf{node}
}

// NOTE: This assumes that search keys will be added in order
func (l *bTreeLeaf) Add(searchKey uint32, rowID RowID) {
	entriesCount := l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)

	// Since we always insert keys in order, we always append the record at the
	// end of the node
	initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (entriesCount * BTREE_LEAF_ENTRY_SIZE))
	l.block.Write(initialOffset+BTREE_LEAF_OFFSET_KEY, searchKey)
	l.block.Write(initialOffset+BTREE_LEAF_OFFSET_BLOCK_ID, rowID.DataBlockID)
	l.block.Write(initialOffset+BTREE_LEAF_OFFSET_LOCAL_ID, rowID.LocalID)

	entriesCount += 1
	l.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount))
}

func (l *bTreeLeaf) Find(searchKey uint32) RowID {
	entriesCount := int(l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	// XXX: Should we perform a binary search here?
	for i := 0; i < entriesCount; i++ {
		ptr := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_LEAF_ENTRY_SIZE))
		keyFound := l.block.ReadUint32(ptr + BTREE_LEAF_OFFSET_KEY)
		if keyFound != searchKey {
			continue
		}
		return RowID{
			RecordID:    searchKey,
			DataBlockID: l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_BLOCK_ID),
			LocalID:     l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_LOCAL_ID),
		}
	}
	return RowID{}
}

func (l *bTreeLeaf) Remove(searchKey uint32) {
	entriesCount := int(l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	// XXX: Should we perform a binary search here?
	entryPtrToRemove := -1
	entryPosition := 0
	for i := 0; i < entriesCount; i++ {
		ptr := int(BTREE_POS_ENTRIES_OFFSET) + int(i*BTREE_LEAF_ENTRY_SIZE)
		keyFound := l.block.ReadUint32(ptr + BTREE_LEAF_OFFSET_KEY)
		if keyFound == searchKey {
			entryPtrToRemove = ptr
			entryPosition = i
			break
		}
	}

	if entryPtrToRemove == -1 {
		panic(fmt.Sprintf("Tried to remove an entry that does not exist on the index: %d", searchKey))
	}

	// Write back the amount of entries on this block
	l.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))

	// If we are removing the last key, just update the entries count and call it a day
	if entryPosition == (entriesCount - 1) {
		return
	}

	// Copy data over
	lastByteToOverwrite := int(BTREE_POS_ENTRIES_OFFSET) + (entriesCount-1)*BTREE_LEAF_ENTRY_SIZE
	for i := entryPtrToRemove; i < lastByteToOverwrite; i++ {
		l.block.Data[i] = l.block.Data[i+BTREE_LEAF_ENTRY_SIZE]
	}
}

func (l *bTreeLeaf) All() []RowID {
	entriesCount := int(l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))
	all := make([]RowID, 0, entriesCount)
	for i := 0; i < entriesCount; i++ {
		ptr := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_LEAF_ENTRY_SIZE))
		all = append(all, RowID{
			RecordID:    l.block.ReadUint32(ptr + BTREE_LEAF_OFFSET_KEY),
			DataBlockID: l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_BLOCK_ID),
			LocalID:     l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_LOCAL_ID),
		})
	}
	return all
}
