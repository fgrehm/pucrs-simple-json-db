package core

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

	"bplustree"
	"simplejsondb/dbio"
)

const (
	BTREE_TYPE_BRANCH = uint8(1)
	BTREE_TYPE_LEAF   = uint8(2)

	BTREE_POS_TYPE           = 0
	BTREE_POS_TOTAL_KEYS     = BTREE_POS_TYPE + 1
	BTREE_POS_PARENT_ID      = BTREE_POS_TOTAL_KEYS + 2
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

type uint32IndexNodeAdapter struct {
	buffer dbio.DataBuffer
	repo   DataBlockRepository
}

type uint32IndexNode struct {
	block   *dbio.DataBlock
	adapter *uint32IndexNodeAdapter
}

type uint32IndexLeafNode struct {
	*uint32IndexNode
}
type uint32IndexBranchNode struct {
	*uint32IndexNode
}

func (a *uint32IndexNodeAdapter) SetRoot(node bplustree.Node) {
	cb := a.repo.ControlBlock()

	nodeID := uint16(node.ID().(Uint16ID))
	log.Infof("IDX_SET_ROOT %d", nodeID)
	node.SetParentID(Uint16ID(0))

	cb.SetIndexRootBlockID(nodeID)
	a.buffer.MarkAsDirty(cb.DataBlockID())
}

func (a *uint32IndexNodeAdapter) Init() bplustree.LeafNode {
	log.Infof("IDX_INIT")
	root := a.CreateLeaf()
	a.SetRoot(root)
	cb := a.repo.ControlBlock()
	cb.SetFirstLeaf(uint16(root.ID().(Uint16ID)))
	a.buffer.MarkAsDirty(cb.DataBlockID())
	return root
}

func (a *uint32IndexNodeAdapter) IsRoot(node bplustree.Node) bool {
	return uint16(node.ParentID().(Uint16ID)) == 0
}

func (a *uint32IndexNodeAdapter) LoadRoot() bplustree.Node {
	cb := a.repo.ControlBlock()
	rootID := cb.IndexRootBlockID()
	if rootID == 0 {
		return nil
	} else {
		return a.LoadNode(Uint16ID(rootID))
	}
}

func (a *uint32IndexNodeAdapter) LoadNode(id bplustree.NodeID) bplustree.Node {
	node := a.loadNode(id)
	if node == nil {
		return nil
	}
	log.Debugf("IDX_LOADED nodeID=%d", id)

	if node.isLeaf() {
		return &uint32IndexLeafNode{node}
	} else {
		return &uint32IndexBranchNode{node}
	}
}

func (a *uint32IndexNodeAdapter) loadNode(id bplustree.NodeID) *uint32IndexNode {
	log.Debugf("IDX_LOAD nodeID=%d", id)
	nodeID := uint16(id.(Uint16ID))
	if nodeID == 0 {
		return nil
	}
	return &uint32IndexNode{block: a.repo.fetchBlock(nodeID), adapter: a}
}

func (a *uint32IndexNodeAdapter) Free(node bplustree.Node) {
	nodeID := uint16(node.ID().(Uint16ID))
	log.Infof("IDX_FREE nodeID=%d", nodeID)
	dataBlocksMap := &dataBlocksMap{a.buffer}
	dataBlocksMap.MarkAsFree(nodeID)
}

func (a *uint32IndexNodeAdapter) CreateLeaf() bplustree.LeafNode {
	block := a.allocateBlock()
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_LEAF)
	a.buffer.MarkAsDirty(block.ID)
	log.Infof("IDX_LEAF_ALLOC nodeID=%d", block.ID)
	return &uint32IndexLeafNode{&uint32IndexNode{block: block, adapter: a}}
}

func (a *uint32IndexNodeAdapter) allocateBlock() *dbio.DataBlock {
	blocksMap := &dataBlocksMap{a.buffer}
	blockID := blocksMap.FirstFree()
	block, err := a.buffer.FetchBlock(blockID)
	if err != nil {
		panic(err)
	}
	blocksMap.MarkAsUsed(blockID)
	block.Write(BTREE_POS_TOTAL_KEYS, uint16(0))
	block.Write(BTREE_POS_PARENT_ID, uint16(0))
	block.Write(BTREE_POS_RIGHT_SIBLING, uint16(0))
	block.Write(BTREE_POS_LEFT_SIBLING, uint16(0))
	return block
}

func (a *uint32IndexNodeAdapter) markAsDirty(node *uint32IndexNode) {
	a.buffer.MarkAsDirty(node.block.ID)
}

func (a *uint32IndexNodeAdapter) LoadFirstLeaf() bplustree.LeafNode {
	cb := a.repo.ControlBlock()
	return a.LoadLeaf(Uint16ID(cb.FirstLeaf()))
}

func (a *uint32IndexNodeAdapter) LoadLeaf(id bplustree.NodeID) bplustree.LeafNode {
	node := a.loadNode(id)
	if node == nil {
		return nil
	} else {
		log.Debugf("IDX_LEAF_LOADED nodeID=%d", id)
		return &uint32IndexLeafNode{node}
	}
}

func (a *uint32IndexNodeAdapter) CreateBranch(entry bplustree.BranchEntry) bplustree.BranchNode {
	block := a.allocateBlock()
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)

	writeOffset := int(BTREE_POS_ENTRIES_OFFSET)
	node := &uint32IndexBranchNode{&uint32IndexNode{block: block, adapter: a}}
	node.block.Write(writeOffset+BTREE_BRANCH_OFFSET_KEY, uint32(entry.Key.(Uint32Key)))
	node.block.Write(writeOffset+BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID, uint16(entry.LowerThanKeyNodeID.(Uint16ID)))
	node.block.Write(writeOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, uint16(entry.GreaterThanOrEqualToKeyNodeID.(Uint16ID)))

	node.block.Write(BTREE_POS_TOTAL_KEYS, uint16(1))

	log.Infof("IDX_BRANCH_ALLOC nodeID=%d, initialEntry=%+v", node.block.ID, entry)

	a.buffer.MarkAsDirty(block.ID)
	return node
}

func (a *uint32IndexNodeAdapter) LoadBranch(id bplustree.NodeID) bplustree.BranchNode {
	node := a.loadNode(id)
	if node == nil {
		return nil
	} else {
		log.Debugf("IDX_BRANCH_LOADED nodeID=%d", id)
		return &uint32IndexBranchNode{node}
	}
}

func (n *uint32IndexNode) isLeaf() bool {
	return n.block.ReadUint8(BTREE_POS_TYPE) == BTREE_TYPE_LEAF
}

func (n *uint32IndexNode) ID() bplustree.NodeID {
	return Uint16ID(n.block.ID)
}

func (n *uint32IndexNode) TotalKeys() int {
	return int(n.block.ReadUint16(BTREE_POS_TOTAL_KEYS))
}

func (n *uint32IndexNode) RightSiblingID() bplustree.NodeID {
	return Uint16ID(n.block.ReadUint16(BTREE_POS_RIGHT_SIBLING))
}

func (n *uint32IndexNode) ParentID() bplustree.NodeID {
	return Uint16ID(n.block.ReadUint16(BTREE_POS_PARENT_ID))
}

func (n *uint32IndexNode) SetParentID(id bplustree.NodeID) {
	log.Infof("IDX_NODE_SET_PARENT nodeID=%d, parentID=%d", id, n.block.ID)
	n.block.Write(BTREE_POS_PARENT_ID, uint16(id.(Uint16ID)))
	n.adapter.markAsDirty(n)
}

func (n *uint32IndexNode) LeftSiblingID() bplustree.NodeID {
	return Uint16ID(n.block.ReadUint16(BTREE_POS_LEFT_SIBLING))
}

func (n *uint32IndexNode) SetLeftSiblingID(id bplustree.NodeID) {
	log.Infof("IDX_NODE_SET_LEFT nodeID=%d, leftID=%d", n.block.ID, id)
	n.block.Write(BTREE_POS_LEFT_SIBLING, uint16(id.(Uint16ID)))
	n.adapter.markAsDirty(n)
}

func (n *uint32IndexNode) SetRightSiblingID(id bplustree.NodeID) {
	log.Infof("IDX_NODE_SET_RIGHT nodeID=%d, rightID=%d", n.block.ID, id)
	n.block.Write(BTREE_POS_RIGHT_SIBLING, uint16(id.(Uint16ID)))
	n.adapter.markAsDirty(n)
}

func (l *uint32IndexLeafNode) InsertAt(position int, entry bplustree.LeafEntry) {
	if position == -1 {
		position = 0
	}

	writeOffset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_LEAF_ENTRY_SIZE)
	totalKeys := l.TotalKeys()
	if position != totalKeys {
		l.block.Unshift(writeOffset, BTREE_LEAF_ENTRY_SIZE)
	}

	log.Printf("IDX_LEAF_INSERT nodeID=%d, position=%d, entry=%+v, offset=%d", l.block.ID, position, entry, writeOffset)

	key := uint32(entry.Key.(Uint32Key))
	rowID := entry.Item.(RowID)
	l.block.Write(writeOffset+BTREE_LEAF_OFFSET_KEY, key)
	l.block.Write(writeOffset+BTREE_LEAF_OFFSET_BLOCK_ID, rowID.DataBlockID)
	l.block.Write(writeOffset+BTREE_LEAF_OFFSET_LOCAL_ID, rowID.LocalID)

	totalKeys += 1
	l.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys))
	l.adapter.markAsDirty(l.uint32IndexNode)
}

func (l *uint32IndexLeafNode) KeyAt(position int) bplustree.Key {
	readOffset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_LEAF_ENTRY_SIZE)
	return Uint32Key(l.block.ReadUint32(readOffset + BTREE_LEAF_OFFSET_KEY))
}

func (l *uint32IndexLeafNode) ItemAt(position int) bplustree.Item {
	readOffset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_LEAF_ENTRY_SIZE)
	return RowID{
		DataBlockID: l.block.ReadUint16(readOffset + BTREE_LEAF_OFFSET_BLOCK_ID),
		LocalID:     l.block.ReadUint16(readOffset + BTREE_LEAF_OFFSET_LOCAL_ID),
	}
}

func (l *uint32IndexLeafNode) DeleteAt(position int) bplustree.LeafEntry {
	totalKeys := l.TotalKeys()
	if position < 0 || position >= totalKeys {
		panic("Invalid position to be deleted")
	}

	log.Printf("IDX_LEAF_DELETE nodeID=%d, position=%d, totalKeys=%d", l.block.ID, position, totalKeys)
	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_LEAF_ENTRY_SIZE)
	entry := l.readEntry(offset)

	copy(l.block.Data[offset:], l.block.Data[offset+BTREE_LEAF_ENTRY_SIZE:])
	l.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys-1))
	l.adapter.markAsDirty(l.uint32IndexNode)

	return entry
}

func (l *uint32IndexLeafNode) DeleteFrom(startPosition int) bplustree.LeafEntries {
	totalKeys := l.TotalKeys()

	log.Printf("IDX_LEAF_DELETE_FROM nodeID=%d, startPosition=%d, totalKeys=%d", l.block.ID, startPosition, totalKeys)

	entries := bplustree.LeafEntries{}
	readOffset := int(BTREE_POS_ENTRIES_OFFSET) + startPosition*int(BTREE_LEAF_ENTRY_SIZE)
	for i := startPosition; i < totalKeys; i++ {
		entries = append(entries, l.readEntry(readOffset))
		readOffset += BTREE_LEAF_ENTRY_SIZE
	}

	l.block.Write(BTREE_POS_TOTAL_KEYS, uint16(startPosition))
	l.adapter.markAsDirty(l.uint32IndexNode)

	return entries
}

func (l *uint32IndexLeafNode) All(iterator bplustree.LeafEntriesIterator) error {
	totalKeys := l.TotalKeys()
	offset := int(BTREE_POS_ENTRIES_OFFSET)
	for i := 0; i < totalKeys; i++ {
		iterator(l.readEntry(offset))
		offset += BTREE_LEAF_ENTRY_SIZE
	}
	return nil
}

func (l *uint32IndexLeafNode) readEntry(entryOffset int) bplustree.LeafEntry {
	return bplustree.LeafEntry{
		Key: Uint32Key(l.block.ReadUint32(entryOffset + BTREE_LEAF_OFFSET_KEY)),
		Item: RowID{
			DataBlockID: l.block.ReadUint16(entryOffset + BTREE_LEAF_OFFSET_BLOCK_ID),
			LocalID:     l.block.ReadUint16(entryOffset + BTREE_LEAF_OFFSET_LOCAL_ID),
		},
	}
}

func (b *uint32IndexBranchNode) KeyAt(position int) bplustree.Key {
	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)
	return Uint32Key(b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY))
}

func (b *uint32IndexBranchNode) EntryAt(position int) bplustree.BranchEntry {
	totalKeys := b.TotalKeys()
	if position < 0 || position >= totalKeys {
		panic(fmt.Sprintf("Invalid position to load: %d (total keys = %d)", position, totalKeys))
	}
	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)
	return b.readEntry(offset)
}

func (b *uint32IndexBranchNode) readEntry(offset int) bplustree.BranchEntry {
	return bplustree.BranchEntry{
		Key:                           Uint32Key(b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)),
		LowerThanKeyNodeID:            Uint16ID(b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID)),
		GreaterThanOrEqualToKeyNodeID: Uint16ID(b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID)),
	}
}

func (b *uint32IndexBranchNode) DeleteAt(position int) bplustree.BranchEntry {
	totalKeys := b.TotalKeys()
	log.Printf("IDX_BRANCH_DELETE nodeID=%d, position=%d, totalKeys=%d", b.block.ID, position, totalKeys)
	if position < 0 || position >= totalKeys {
		panic("Invalid position to be deleted")
	}

	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)
	entry := b.readEntry(offset)

	if position == totalKeys-1 {
		b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys-1))
		return entry
	}

	copy(b.block.Data[offset:], b.block.Data[offset+BTREE_BRANCH_ENTRY_JUMP:])
	b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys-1))

	return entry
}

func (b *uint32IndexBranchNode) ReplaceKeyAt(position int, key bplustree.Key) {
	totalKeys := b.TotalKeys()
	if position < 0 || position >= totalKeys {
		panic("Invalid position to be replaced")
	}

	uint32Key := uint32(key.(Uint32Key))
	log.Printf("IDX_BRANCH_REPLACE_KEY nodeID=%d, position=%d, newKey=%d", b.block.ID, position, uint32Key)

	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)+int(BTREE_BRANCH_OFFSET_KEY)
	b.block.Write(offset, uint32Key)
}

func (b *uint32IndexBranchNode) DeleteFrom(startPosition int) bplustree.BranchEntries {
	totalKeys := b.TotalKeys()

	log.Printf("IDX_BRANCH_DELETE_FROM nodeID=%d, startPosition=%d, totalKeys=%d", b.block.ID, startPosition, totalKeys)

	entries := bplustree.BranchEntries{}
	readOffset := int(BTREE_POS_ENTRIES_OFFSET) + startPosition*int(BTREE_BRANCH_ENTRY_JUMP)
	for i := startPosition; i < totalKeys; i++ {
		entries = append(entries, b.readEntry(readOffset))
		readOffset += BTREE_BRANCH_ENTRY_JUMP
	}

	b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(startPosition))
	b.adapter.markAsDirty(b.uint32IndexNode)

	return entries
}

func (b *uint32IndexBranchNode) Shift() {
	log.Printf("IDX_BRANCH_SHIFT nodeID=%d", b.block.ID)
	offset := int(BTREE_POS_ENTRIES_OFFSET) + BTREE_BRANCH_OFFSET_KEY
	copy(b.block.Data[offset:], b.block.Data[offset+BTREE_BRANCH_ENTRY_JUMP:])
	totalKeys := int(b.block.ReadUint16(BTREE_POS_TOTAL_KEYS))
	b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys-1))
}

func (b *uint32IndexBranchNode) All(iterator bplustree.BranchEntriesIterator) error {
	totalKeys := b.TotalKeys()
	offset := int(BTREE_POS_ENTRIES_OFFSET)
	for i := 0; i < totalKeys; i++ {
		entry := b.readEntry(offset)
		iterator(entry)
		offset += BTREE_BRANCH_ENTRY_JUMP
	}
	return nil
}

func (b *uint32IndexBranchNode) InsertAt(position int, key bplustree.Key, greaterThanOrEqualToKeyNodeID bplustree.NodeID) {
	if position == -1 {
		panic("Unexpected insert on branch position")
	}

	uint32Key := uint32(key.(Uint32Key))
	gteNodeID := uint16(greaterThanOrEqualToKeyNodeID.(Uint16ID))
	writeOffset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)

	log.Printf("IDX_BRANCH_INSERT nodeID=%d, position=%d, key=%d, gteNodeID=%d, offset=%d", b.block.ID, position, uint32Key, gteNodeID, writeOffset)

	// When we add an entry to a branch, we keep the LowerThanKeyNodeID around.
	// In order to update it we should use the Unshift method
	b.block.Unshift(writeOffset+int(BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID), BTREE_BRANCH_ENTRY_JUMP)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_KEY, uint32Key)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, gteNodeID)

	totalKeys := b.TotalKeys() + 1
	b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys))
	b.adapter.markAsDirty(b.uint32IndexNode)
}

func (b *uint32IndexBranchNode) Unshift(key bplustree.Key, lowerThanKeyNodeID bplustree.NodeID) {
	uint32Key := uint32(key.(Uint32Key))
	ltKeyNodeID := uint16(lowerThanKeyNodeID.(Uint16ID))
	writeOffset := int(BTREE_POS_ENTRIES_OFFSET)

	b.block.Unshift(writeOffset+int(BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID), BTREE_BRANCH_ENTRY_JUMP)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_KEY, uint32Key)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID, ltKeyNodeID)

	totalKeys := b.TotalKeys() + 1
	b.block.Write(BTREE_POS_TOTAL_KEYS, uint16(totalKeys))

	b.adapter.markAsDirty(b.uint32IndexNode)
}
