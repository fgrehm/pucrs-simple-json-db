package core

import (
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
	block *dbio.DataBlock
	//   parentID Uint16ID
	//   leftID   Uint16ID
	//   rightID  Uint16ID
	//   entries  LeafEntries
}

func (n *uint32IndexNode) isLeaf() bool {
	return n.block.ReadUint8(BTREE_POS_TYPE) == BTREE_TYPE_LEAF
}

func (n *uint32IndexNode) TotalKeys() int {
	return int(n.block.ReadUint16(BTREE_POS_TOTAL_KEYS))
}

func (n *uint32IndexNode) RightSiblingID() bplustree.NodeID {
	return Uint16ID(n.block.ReadUint16(BTREE_POS_RIGHT_SIBLING))
}

type uint32IndexLeafNode struct {
	*uint32IndexNode
}
type uint32IndexBranchNode struct {
	*uint32IndexNode
}

func (a *uint32IndexNodeAdapter) SetRoot(node bplustree.Node) {
	panic("NOT WORKING YET")
	// a.rootID = node.ID().(Uint16ID)
	// node.SetParentID(Uint16ID(0))
}

func (a *uint32IndexNodeAdapter) Init() bplustree.LeafNode {
	root := a.CreateLeaf()
	a.SetRoot(root)
	cb := a.repo.ControlBlock()
	cb.SetFirstLeaf(uint16(root.ID().(Uint16ID)))
	return root
}

func (a *uint32IndexNodeAdapter) IsRoot(node bplustree.Node) bool {
	panic("NOT WORKING YET")
	// return a.rootID == node.ID()
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
	return a.repo.IndexNode(uint16(id.(Uint16ID)))
}

func (a *uint32IndexNodeAdapter) Free(node bplustree.Node) {
	panic("NOT WORKING YET")
	// delete(a.Nodes, node.ID().(Uint16ID))
}

func (a *uint32IndexNodeAdapter) LoadFirstLeaf() bplustree.LeafNode {
	cb := a.repo.ControlBlock()
	return a.LoadLeaf(Uint16ID(cb.FirstLeaf()))
}

func (a *uint32IndexNodeAdapter) LoadBranch(id bplustree.NodeID) bplustree.BranchNode {
	panic("NOT WORKING YET")
}

func (a *uint32IndexNodeAdapter) LoadLeaf(id bplustree.NodeID) bplustree.LeafNode {
	return a.repo.IndexLeaf(uint16(id.(Uint16ID)))
}

func (a *uint32IndexNodeAdapter) CreateLeaf() bplustree.LeafNode {
	panic("NOT WORKING YET")
	// node := &uint32IndexLeafNode{id: Uint16ID(a.nextNodeID)}
	// a.Nodes[node.id] = node
	// a.nextNodeID += 1
	// return node

	// block := a.buffer.FetchBlock
	// block.Write(BTREE_POS_TYPE, BTREE_TYPE_LEAF)
	// node := &uint32IndexLeafNode{block}
	// node := &bTreeNode{block}
	// return &bTreeLeaf{node}
}

func (a *uint32IndexNodeAdapter) CreateBranch(entry bplustree.BranchEntry) bplustree.BranchNode {
	panic("NOT WORKING YET")
	// node := &uint32IndexBranchNode{id: Uint16ID(a.nextNodeID)}
	// node.entries = BranchEntries{entry}
	// a.Nodes[node.id] = node
	// a.nextNodeID += 1
	// return node
}

func (l *uint32IndexLeafNode) ID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(l.id)
}

func (l *uint32IndexLeafNode) ParentID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(l.parentID)
}

func (l *uint32IndexLeafNode) SetParentID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// l.parentID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) LeftSiblingID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(l.leftID)
}

func (l *uint32IndexLeafNode) SetLeftSiblingID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// l.leftID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) SetRightSiblingID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// l.rightID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) InsertAt(position int, entry bplustree.LeafEntry) {
	panic("NOT WORKING YET")
	// if position == len(l.entries) {
	//   l.entries = append(l.entries, entry)
	// } else if position == 0 {
	//   l.entries = append(LeafEntries{entry}, l.entries...)
	// } else {
	//   l.entries = append(l.entries, entry)
	//   copy(l.entries[position+1:], l.entries[position:])
	//   l.entries[position] = entry
	// }
}

func (l *uint32IndexLeafNode) KeyAt(position int) bplustree.Key {
	panic("NOT WORKING YET")
	// return l.entries[position].Key
}

func (l *uint32IndexLeafNode) ItemAt(position int) bplustree.Item {
	panic("NOT WORKING YET")
	// return l.entries[position].Item
}

func (l *uint32IndexLeafNode) DeleteAt(position int) bplustree.LeafEntry {
	panic("NOT WORKING YET")
	// deleted := l.entries[position]
	// l.entries = append(l.entries[:position], l.entries[position+1:]...)
	// return deleted
}

func (l *uint32IndexLeafNode) DeleteFrom(startPosition int) bplustree.LeafEntries {
	panic("NOT WORKING YET")
	// deleted := l.entries[startPosition:]
	// l.entries = l.entries[0:startPosition]
	// return deleted
}

func (l *uint32IndexLeafNode) All(iterator bplustree.LeafEntriesIterator) error {
	totalKeys := l.TotalKeys()
	println("AQUI", totalKeys)
	println("DBLOCK ID", l.block.ID)
	offset := int(BTREE_POS_ENTRIES_OFFSET)
	for i := 0; i < totalKeys; i++ {
		iterator(l.readEntry(offset))
		offset += BTREE_BRANCH_ENTRY_JUMP
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

func (b *uint32IndexBranchNode) ID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(b.id)
}

func (b *uint32IndexBranchNode) ParentID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(b.parentID)
}

func (b *uint32IndexBranchNode) SetParentID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// b.parentID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) LeftSiblingID() bplustree.NodeID {
	panic("NOT WORKING YET")
	// return bplustree.NodeID(b.leftID)
}

func (b *uint32IndexBranchNode) SetLeftSiblingID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// b.leftID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) SetRightSiblingID(id bplustree.NodeID) {
	panic("NOT WORKING YET")
	// b.rightID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) KeyAt(position int) bplustree.Key {
	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)
	return Uint32Key(b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY))
}

func (b *uint32IndexBranchNode) EntryAt(position int) bplustree.BranchEntry {
	offset := int(BTREE_POS_ENTRIES_OFFSET) + position*int(BTREE_BRANCH_ENTRY_JUMP)
	return bplustree.BranchEntry{
		Key:                           Uint32Key(b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)),
		LowerThanKeyNodeID:            Uint16ID(b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID)),
		GreaterThanOrEqualToKeyNodeID: Uint16ID(b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID)),
	}
}

func (b *uint32IndexBranchNode) Append(key bplustree.Key, gteNodeID bplustree.NodeID) {
	panic("NOT WORKING YET")
	// entry := bplustree.BranchEntry{
	//   bplustree.Key:                           key,
	//   LowerThanKeyNodeID:            b.entries[len(b.entries)-1].LowerThanKeyNodeID,
	//   GreaterThanOrEqualToKeyNodeID: gteNodeID,
	// }
	// b.entries = append(b.entries, entry)
}

func (l *uint32IndexBranchNode) DeleteAt(position int) bplustree.BranchEntry {
	panic("NOT WORKING YET")
	// entry := l.entries[position]
	// if position == len(l.entries)-1 {
	//   l.entries = l.entries[0:position]
	// } else if position == 0 {
	//   l.entries = l.entries[1:]
	// } else {
	//   l.entries[position+1].LowerThanKeyNodeID = l.entries[position-1].GreaterThanOrEqualToKeyNodeID
	//   l.entries = append(l.entries[:position], l.entries[position+1:]...)
	// }
	// return entry
}

func (b *uint32IndexBranchNode) ReplaceKeyAt(position int, key bplustree.Key) {
	panic("NOT WORKING YET")
	// b.entries[position].Key = key
}

func (b *uint32IndexBranchNode) DeleteFrom(startPosition int) bplustree.BranchEntries {
	panic("NOT WORKING YET")
	// removed := b.entries[startPosition:]
	// b.entries = b.entries[0:startPosition]
	// return removed
}

func (b *uint32IndexBranchNode) Shift() {
	panic("NOT WORKING YET")
	// ltNodeID := b.entries[0].LowerThanKeyNodeID
	// b.entries = b.entries[1:]
	// b.entries[0].LowerThanKeyNodeID = ltNodeID
}

func (b *uint32IndexBranchNode) All(iterator bplustree.BranchEntriesIterator) error {
	panic("NOT WORKING YET")
	// for _, entry := range b.entries {
	//   iterator(entry)
	// }
}

func (b *uint32IndexBranchNode) InsertAt(position int, key bplustree.Key, greaterThanOrEqualToKeyNodeID bplustree.NodeID) {
	panic("NOT WORKING YET")
	// entry := bplustree.BranchEntry{
	//   bplustree.Key: key,
	//   GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
	// }

	// if position < 0 {
	//   panic("IS THIS CORRECT?")
	// } else if position == 0 {
	//   entry.LowerThanKeyNodeID = b.entries[0].LowerThanKeyNodeID
	//   b.entries[0].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
	//   b.entries = append(BranchEntries{entry}, b.entries...)
	// } else if position == len(b.entries) {
	//   entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
	//   b.entries = append(b.entries, entry)
	// } else {
	//   entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
	//   b.entries[position].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
	//   b.entries = append(b.entries, entry)
	//   copy(b.entries[position+1:], b.entries[position:])
	//   b.entries[position] = entry
	// }
}

func (b *uint32IndexBranchNode) Unshift(key bplustree.Key, lowerThanKeyNodeID bplustree.NodeID) {
	panic("NOT WORKING YET")
	// entry := bplustree.BranchEntry{
	//   bplustree.Key:                           key,
	//   LowerThanKeyNodeID:            lowerThanKeyNodeID,
	//   GreaterThanOrEqualToKeyNodeID: b.entries[0].LowerThanKeyNodeID,
	// }
	// b.entries = append(BranchEntries{entry}, b.entries...)
}
