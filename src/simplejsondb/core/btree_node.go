package core

// We might want to extract the specific node types out to separate files  if
// the code grows too big

import (
	"fmt"

	"simplejsondb/dbio"

	log "github.com/Sirupsen/logrus"
)

const (
	BTREE_TYPE_BRANCH = uint8(1)
	BTREE_TYPE_LEAF   = uint8(2)

	BTREE_POS_TYPE           = 0
	BTREE_POS_ENTRIES_COUNT  = BTREE_POS_TYPE + 1
	BTREE_POS_PARENT_ID      = BTREE_POS_ENTRIES_COUNT + 2
	BTREE_POS_LEFT_SIBLING   = BTREE_POS_PARENT_ID + 2
	BTREE_POS_RIGHT_SIBLING  = BTREE_POS_LEFT_SIBLING + 2
	BTREE_POS_ENTRIES_OFFSET = BTREE_POS_RIGHT_SIBLING + 2

	BTREE_BRANCH_MAX_ENTRIES           = 680
	BTREE_BRANCH_ENTRY_JUMP            = 6 // 2 bytes for the left pointer and 4 bytes for the search key
	BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID  = 0
	BTREE_BRANCH_OFFSET_KEY            = 2
	BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID = 6

	BTREE_LEAF_MAX_ENTRIES     = 510
	BTREE_LEAF_ENTRY_SIZE      = 8
	BTREE_LEAF_OFFSET_KEY      = 0
	BTREE_LEAF_OFFSET_BLOCK_ID = 4
	BTREE_LEAF_OFFSET_LOCAL_ID = 6
)

type BTreeNode interface {
	DataBlockID() uint16
	IsLeaf() bool
	IsRoot() bool
	Parent() uint16
	EntriesCount() uint16
	ResetPointers()
	SetParent(node BTreeNode)
	SetLeftSibling(node BTreeNode)
	SetRightSibling(node BTreeNode)
	RightSibling() uint16
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

func (n *bTreeNode) EntriesCount() uint16 {
	return n.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)
}

func (n *bTreeNode) IsRoot() bool {
	return n.block.ReadUint16(BTREE_POS_PARENT_ID) == 0
}

func (n *bTreeNode) SetParent(node BTreeNode) {
	n.block.Write(BTREE_POS_PARENT_ID, node.DataBlockID())
}

func (n *bTreeNode) Parent() uint16 {
	return n.block.ReadUint16(BTREE_POS_PARENT_ID)
}

func (n *bTreeNode) SetLeftSibling(node BTreeNode) {
	n.block.Write(BTREE_POS_LEFT_SIBLING, node.DataBlockID())
}

func (n *bTreeNode) SetRightSibling(node BTreeNode) {
	n.block.Write(BTREE_POS_RIGHT_SIBLING, node.DataBlockID())
}

func (n *bTreeNode) RightSibling() uint16 {
	return n.block.ReadUint16(BTREE_POS_RIGHT_SIBLING)
}

func (n *bTreeNode) ResetPointers() {
	n.block.Write(BTREE_POS_PARENT_ID, uint16(0))
	n.block.Write(BTREE_POS_LEFT_SIBLING, uint16(0))
	n.block.Write(BTREE_POS_RIGHT_SIBLING, uint16(0))
}

type BTreeBranch interface {
	BTreeNode
	Add(searchKey uint32, leftNode, rightNode BTreeNode)
	Remove(searchKey uint32)
	ReplaceKey(oldValue, newValue uint32)
	Find(searchKey uint32) uint16
}

type bTreeBranch struct {
	*bTreeNode
}

func CreateBTreeBranch(block *dbio.DataBlock) BTreeBranch {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)
	node := &bTreeNode{block}
	return &bTreeBranch{node}
}

func (b *bTreeBranch) Find(searchKey uint32) uint16 {
	log.Printf("BRANCH_FIND blockid=%d, searchkey=%d", b.block.ID, searchKey)
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	if entriesCount == 0 {
		return 0
	}

	// XXX: Should we perform a binary search here?
	entryToFollowKey := uint32(0)
	entryToFollowPtr := 0
	for i := 0; i < entriesCount; i++ {
		initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_BRANCH_ENTRY_JUMP))
		keyFound := b.block.ReadUint32(initialOffset + BTREE_BRANCH_OFFSET_KEY)

		// We have a match!
		if keyFound >= searchKey {
			entryToFollowKey = keyFound
			entryToFollowPtr = initialOffset
			break
		}
	}

	if entryToFollowPtr == 0 {
		return 0
	}

	if searchKey >= entryToFollowKey {
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID)
	} else {
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID)
	}
}

func (b *bTreeBranch) Add(searchKey uint32, leftNode, rightNode BTreeNode) {
	log.Printf("BRANCH_ADD blockid=%d, searchkey=%d, leftid=%d, rightid=%d", b.block.ID, searchKey, leftNode.DataBlockID(), rightNode.DataBlockID())

	entriesCount := b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)

	// Since we always insert keys in order, we always append the values at the
	// end of the node
	initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (entriesCount * BTREE_BRANCH_ENTRY_JUMP))
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID, leftNode.DataBlockID())
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_KEY, searchKey)
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, rightNode.DataBlockID())

	entriesCount += 1
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount))
}

func (b *bTreeBranch) ReplaceKey(oldValue, newValue uint32) {
	panic("TODO: ReplaceKey")
}

func (b *bTreeBranch) Remove(searchKey uint32) {
	log.Printf("BRANCH_REMOVE blockid=%d, searchkey=%d", b.block.ID, searchKey)

	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	// If there is only one entry on the node, just update the counter
	if entriesCount == 1 {
		b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(0))
		return
	}

	// XXX: Should we perform a binary search here?
	entryToRemovePtr := 0
	entryPosition := 0
	for i := 0; i < entriesCount; i++ {
		initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_BRANCH_ENTRY_JUMP))
		keyFound := b.block.ReadUint32(initialOffset + BTREE_BRANCH_OFFSET_KEY)

		// We have a match!
		if keyFound >= searchKey {
			entryToRemovePtr = initialOffset
			break
		}
	}

	if entryToRemovePtr == 0 {
		panic(fmt.Sprintf("Unable to remove an entry with the key %d", searchKey))
	}

	if entryToRemovePtr == -1 {
		panic(fmt.Sprintf("Tried to remove an entry that does not exist on the index: %d", searchKey))
	}

	// Write back the amount of entries on this block
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))

	// If we are removing the last key, just update the entries count and call it a day
	if entryPosition == (entriesCount - 1) {
		return
	}

	// Copy data over
	lastByteToOverwrite := int(BTREE_POS_ENTRIES_OFFSET) + (entriesCount-1)*BTREE_BRANCH_ENTRY_JUMP
	for i := entryToRemovePtr; i < lastByteToOverwrite; i++ {
		b.block.Data[i] = b.block.Data[i+BTREE_BRANCH_ENTRY_JUMP]
	}
}

type BTreeLeaf interface {
	BTreeNode
	Add(searchKey uint32, rowID RowID)
	Remove(searchKey uint32)
	Shift() RowID
	Find(searchKey uint32) RowID
	First() RowID
	All() []RowID
	IsFull() bool
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
	log.Printf("LEAF_ADD blockid=%d, searchkey=%d, rowid=%+v", l.block.ID, searchKey, rowID)

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
	log.Printf("LEAF_FIND blockid=%d, searchkey=%d", l.block.ID, searchKey)
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
	log.Printf("LEAF_REMOVE blockid=%d, searchkey=%d", l.block.ID, searchKey)

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

func (l *bTreeLeaf) First() RowID {
	entriesCount := int(l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))
	if entriesCount == 0 {
		panic("Called First() on a leaf that has no entries")
	}
	ptr := int(BTREE_POS_ENTRIES_OFFSET + BTREE_LEAF_ENTRY_SIZE)
	return RowID{
		RecordID:    l.block.ReadUint32(ptr + BTREE_LEAF_OFFSET_KEY),
		DataBlockID: l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_BLOCK_ID),
		LocalID:     l.block.ReadUint16(ptr + BTREE_LEAF_OFFSET_LOCAL_ID),
	}
}

func (l *bTreeLeaf) Shift() RowID {
	entriesCount := int(l.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))
	if entriesCount == 0 {
		panic("Called Shift() on a leaf that has no entries")
	}
	first := l.First()
	l.Remove(first.RecordID)
	return first
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

func (n *bTreeLeaf) IsFull() bool {
	return n.block.ReadUint16(BTREE_POS_ENTRIES_COUNT) == BTREE_LEAF_MAX_ENTRIES
}
