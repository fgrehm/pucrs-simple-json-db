package bplustree_test

import (
	"bplustree"
)

type InMemoryAdapter struct {
	rootID      Uint16ID
	firstLeafID Uint16ID
	nodes       map[Uint16ID]bplustree.Node
	nextNodeID  Uint16ID
}

func newInMemoryAdapter() *InMemoryAdapter {
	return &InMemoryAdapter{
		rootID:     9999,
		nodes:      make(map[Uint16ID]bplustree.Node),
		nextNodeID: 1,
	}
}

type Uint32Key uint32

func (a Uint32Key) Less(b bplustree.Key) bool {
	return a < b.(Uint32Key)
}

type Uint16ID uint16
type StringItem string

type inMemoryLeaf struct {
	id       Uint16ID
	parentID Uint16ID
	leftID   Uint16ID
	rightID  Uint16ID
	entries  bplustree.LeafEntries
}
type inMemoryBranch struct {
	id       Uint16ID
	parentID Uint16ID
	leftID   Uint16ID
	rightID  Uint16ID
	entries  bplustree.BranchEntries
}

func (a *InMemoryAdapter) SetRoot(node bplustree.Node) {
	a.rootID = node.ID().(Uint16ID)
	node.SetParentID(Uint16ID(0))
}

func (a *InMemoryAdapter) Init() bplustree.LeafNode {
	root := a.CreateLeaf()
	a.rootID = root.ID().(Uint16ID)
	a.firstLeafID = a.rootID
	return root
}

func (a *InMemoryAdapter) IsRoot(node bplustree.Node) bool {
	return a.rootID == node.ID()
}

func (a *InMemoryAdapter) LoadRoot() bplustree.Node {
	return a.nodes[a.rootID]
}

func (a *InMemoryAdapter) LoadNode(id bplustree.NodeID) bplustree.Node {
	node := a.nodes[id.(Uint16ID)]
	if node != nil {
		return node
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) Free(node bplustree.Node) {
	delete(a.nodes, node.ID().(Uint16ID))
}

func (a *InMemoryAdapter) LoadFirstLeaf() bplustree.LeafNode {
	return a.nodes[a.firstLeafID].(bplustree.LeafNode)
}

func (a *InMemoryAdapter) LoadBranch(id bplustree.NodeID) bplustree.BranchNode {
	node := a.LoadNode(id)
	if node != nil {
		return node.(bplustree.BranchNode)
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) LoadLeaf(id bplustree.NodeID) bplustree.LeafNode {
	node := a.LoadNode(id)
	if node != nil {
		return node.(bplustree.LeafNode)
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) CreateLeaf() bplustree.LeafNode {
	node := &inMemoryLeaf{id: Uint16ID(a.nextNodeID)}
	a.nodes[node.id] = node
	a.nextNodeID += 1
	return node
}

func (a *InMemoryAdapter) CreateBranch(entry bplustree.BranchEntry) bplustree.BranchNode {
	node := &inMemoryBranch{id: Uint16ID(a.nextNodeID)}
	node.entries = bplustree.BranchEntries{entry}
	a.nodes[node.id] = node
	a.nextNodeID += 1
	return node
}

func (l *inMemoryLeaf) ID() bplustree.NodeID {
	return bplustree.NodeID(l.id)
}

func (l *inMemoryLeaf) ParentID() bplustree.NodeID {
	return bplustree.NodeID(l.parentID)
}

func (l *inMemoryLeaf) SetParentID(id bplustree.NodeID) {
	l.parentID = id.(Uint16ID)
}

func (l *inMemoryLeaf) LeftSiblingID() bplustree.NodeID {
	return bplustree.NodeID(l.leftID)
}

func (l *inMemoryLeaf) SetLeftSiblingID(id bplustree.NodeID) {
	l.leftID = id.(Uint16ID)
}

func (l *inMemoryLeaf) RightSiblingID() bplustree.NodeID {
	return bplustree.NodeID(l.rightID)
}

func (l *inMemoryLeaf) SetRightSiblingID(id bplustree.NodeID) {
	l.rightID = id.(Uint16ID)
}

func (l *inMemoryLeaf) TotalKeys() int {
	return len(l.entries)
}

func (l *inMemoryLeaf) InsertAt(position int, entry bplustree.LeafEntry) {
	if position == len(l.entries) {
		l.entries = append(l.entries, entry)
	} else if position == 0 {
		l.entries = append(bplustree.LeafEntries{entry}, l.entries...)
	} else {
		l.entries = append(l.entries, entry)
		copy(l.entries[position+1:], l.entries[position:])
		l.entries[position] = entry
	}
}

func (l *inMemoryLeaf) KeyAt(position int) bplustree.Key {
	return l.entries[position].Key
}

func (l *inMemoryLeaf) ItemAt(position int) bplustree.Item {
	return l.entries[position].Item
}

func (l *inMemoryLeaf) DeleteAt(position int) bplustree.LeafEntry {
	deleted := l.entries[position]
	l.entries = append(l.entries[:position], l.entries[position+1:]...)
	return deleted
}

func (l *inMemoryLeaf) DeleteFrom(startPosition int) bplustree.LeafEntries {
	deleted := l.entries[startPosition:]
	l.entries = l.entries[0:startPosition]
	return deleted
}

func (l *inMemoryLeaf) All(iterator bplustree.LeafEntriesIterator) error {
	for _, entry := range l.entries {
		iterator(entry)
	}
	return nil
}

func (b *inMemoryBranch) ID() bplustree.NodeID {
	return bplustree.NodeID(b.id)
}

func (b *inMemoryBranch) ParentID() bplustree.NodeID {
	return bplustree.NodeID(b.parentID)
}

func (b *inMemoryBranch) SetParentID(id bplustree.NodeID) {
	b.parentID = id.(Uint16ID)
}

func (b *inMemoryBranch) LeftSiblingID() bplustree.NodeID {
	return bplustree.NodeID(b.leftID)
}

func (b *inMemoryBranch) SetLeftSiblingID(id bplustree.NodeID) {
	b.leftID = id.(Uint16ID)
}

func (b *inMemoryBranch) RightSiblingID() bplustree.NodeID {
	return bplustree.NodeID(b.rightID)
}

func (b *inMemoryBranch) SetRightSiblingID(id bplustree.NodeID) {
	b.rightID = id.(Uint16ID)
}

func (b *inMemoryBranch) KeyAt(position int) bplustree.Key {
	return b.entries[position].Key
}

func (b *inMemoryBranch) EntryAt(position int) bplustree.BranchEntry {
	return b.entries[position]
}

func (b *inMemoryBranch) Append(key bplustree.Key, gteNodeID bplustree.NodeID) {
	entry := bplustree.BranchEntry{
		Key:                           key,
		LowerThanKeyNodeID:            b.entries[len(b.entries)-1].LowerThanKeyNodeID,
		GreaterThanOrEqualToKeyNodeID: gteNodeID,
	}
	b.entries = append(b.entries, entry)
}

func (l *inMemoryBranch) DeleteAt(position int) {
	if position == len(l.entries) - 1 {
		l.entries = l.entries[0:position]
	} else {
		l.entries[position+1].LowerThanKeyNodeID = l.entries[position-1].GreaterThanOrEqualToKeyNodeID
		l.entries = append(l.entries[:position], l.entries[position+1:]...)
	}
}

func (b *inMemoryBranch) ReplaceKeyAt(position int, key bplustree.Key) {
	b.entries[position].Key = key
}

func (b *inMemoryBranch) DeleteFrom(startPosition int) bplustree.BranchEntries {
	removed := b.entries[startPosition:]
	b.entries = b.entries[0:startPosition]
	return removed
}

func (b *inMemoryBranch) Shift() {
	ltNodeID := b.entries[0].LowerThanKeyNodeID
	b.entries = b.entries[1:]
	b.entries[0].LowerThanKeyNodeID = ltNodeID
}

func (b *inMemoryBranch) TotalKeys() int {
	return len(b.entries)
}

func (b *inMemoryBranch) All(iterator bplustree.BranchEntriesIterator) error {
	for _, entry := range b.entries {
		iterator(entry)
	}
	return nil
}

func (b *inMemoryBranch) InsertAt(position int, key bplustree.Key, greaterThanOrEqualToKeyNodeID bplustree.NodeID) {
	if position == 0 {
		lowerThanKeyNodeID := b.entries[0].LowerThanKeyNodeID
		entry := bplustree.BranchEntry{
			Key:                           key,
			LowerThanKeyNodeID:            lowerThanKeyNodeID,
			GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
		}
		b.entries[0].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
		b.entries = append(bplustree.BranchEntries{entry}, b.entries...)

	} else if position == len(b.entries) {
		lowerThanKeyNodeID := b.entries[position-1].GreaterThanOrEqualToKeyNodeID
		entry := bplustree.BranchEntry{
			Key:                           key,
			LowerThanKeyNodeID:            lowerThanKeyNodeID,
			GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
		}
		b.entries = append(b.entries, entry)
	} else {
		lowerThanKeyNodeID := b.entries[position].GreaterThanOrEqualToKeyNodeID
		entry := bplustree.BranchEntry{
			Key:                           key,
			LowerThanKeyNodeID:            lowerThanKeyNodeID,
			GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
		}
		b.entries[position].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID

		b.entries = append(b.entries, entry)
		copy(b.entries[position+1:], b.entries[position:])
		b.entries[position] = entry
	}
}
