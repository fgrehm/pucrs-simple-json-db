package bplustree

type InMemoryAdapter struct {
	rootID      Uint16ID
	firstLeafID Uint16ID
	nextNodeID  Uint16ID
	Nodes       map[Uint16ID]Node
}

func NewInMemoryAdapter() *InMemoryAdapter {
	return &InMemoryAdapter{
		rootID:     9999,
		nextNodeID: 1,
		Nodes:      make(map[Uint16ID]Node),
	}
}

type Uint32Key uint32

func (k Uint32Key) Less(other Key) bool {
	return k < other.(Uint32Key)
}

type Uint16ID uint16

func (i Uint16ID) Equals(other NodeID) bool {
	return i == other.(Uint16ID)
}

type StringItem string

type inMemoryLeaf struct {
	id       Uint16ID
	parentID Uint16ID
	leftID   Uint16ID
	rightID  Uint16ID
	entries  LeafEntries
}
type inMemoryBranch struct {
	id       Uint16ID
	parentID Uint16ID
	leftID   Uint16ID
	rightID  Uint16ID
	entries  BranchEntries
}

func (a *InMemoryAdapter) SetRoot(node Node) {
	a.rootID = node.ID().(Uint16ID)
	node.SetParentID(Uint16ID(0))
}

func (a *InMemoryAdapter) Init() LeafNode {
	root := a.CreateLeaf()
	a.rootID = root.ID().(Uint16ID)
	a.firstLeafID = a.rootID
	return root
}

func (a *InMemoryAdapter) IsRoot(node Node) bool {
	return a.rootID == node.ID()
}

func (a *InMemoryAdapter) LoadRoot() Node {
	return a.Nodes[a.rootID]
}

func (a *InMemoryAdapter) LoadNode(id NodeID) Node {
	node := a.Nodes[id.(Uint16ID)]
	if node != nil {
		return node
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) Free(node Node) {
	delete(a.Nodes, node.ID().(Uint16ID))
}

func (a *InMemoryAdapter) LoadFirstLeaf() LeafNode {
	return a.Nodes[a.firstLeafID].(LeafNode)
}

func (a *InMemoryAdapter) LoadBranch(id NodeID) BranchNode {
	node := a.LoadNode(id)
	if node != nil {
		return node.(BranchNode)
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) LoadLeaf(id NodeID) LeafNode {
	node := a.LoadNode(id)
	if node != nil {
		return node.(LeafNode)
	} else {
		return nil
	}
}

func (a *InMemoryAdapter) CreateLeaf() LeafNode {
	node := &inMemoryLeaf{id: Uint16ID(a.nextNodeID)}
	a.Nodes[node.id] = node
	a.nextNodeID += 1
	return node
}

func (a *InMemoryAdapter) CreateBranch(entry BranchEntry) BranchNode {
	node := &inMemoryBranch{id: Uint16ID(a.nextNodeID)}
	node.entries = BranchEntries{entry}
	a.Nodes[node.id] = node
	a.nextNodeID += 1
	return node
}

func (l *inMemoryLeaf) ID() NodeID {
	return NodeID(l.id)
}

func (l *inMemoryLeaf) ParentID() NodeID {
	return NodeID(l.parentID)
}

func (l *inMemoryLeaf) SetParentID(id NodeID) {
	l.parentID = id.(Uint16ID)
}

func (l *inMemoryLeaf) LeftSiblingID() NodeID {
	return NodeID(l.leftID)
}

func (l *inMemoryLeaf) SetLeftSiblingID(id NodeID) {
	l.leftID = id.(Uint16ID)
}

func (l *inMemoryLeaf) RightSiblingID() NodeID {
	return NodeID(l.rightID)
}

func (l *inMemoryLeaf) SetRightSiblingID(id NodeID) {
	l.rightID = id.(Uint16ID)
}

func (l *inMemoryLeaf) TotalKeys() int {
	return len(l.entries)
}

func (l *inMemoryLeaf) InsertAt(position int, entry LeafEntry) {
	if position == len(l.entries) {
		l.entries = append(l.entries, entry)
	} else if position == 0 {
		l.entries = append(LeafEntries{entry}, l.entries...)
	} else {
		l.entries = append(l.entries, entry)
		copy(l.entries[position+1:], l.entries[position:])
		l.entries[position] = entry
	}
}

func (l *inMemoryLeaf) KeyAt(position int) Key {
	return l.entries[position].Key
}

func (l *inMemoryLeaf) ItemAt(position int) Item {
	return l.entries[position].Item
}

func (l *inMemoryLeaf) DeleteAt(position int) LeafEntry {
	deleted := l.entries[position]
	l.entries = append(l.entries[:position], l.entries[position+1:]...)
	return deleted
}

func (l *inMemoryLeaf) DeleteFrom(startPosition int) LeafEntries {
	deleted := l.entries[startPosition:]
	l.entries = l.entries[0:startPosition]
	return deleted
}

func (l *inMemoryLeaf) All(iterator LeafEntriesIterator) error {
	for _, entry := range l.entries {
		iterator(entry)
	}
	return nil
}

func (b *inMemoryBranch) ID() NodeID {
	return NodeID(b.id)
}

func (b *inMemoryBranch) ParentID() NodeID {
	return NodeID(b.parentID)
}

func (b *inMemoryBranch) SetParentID(id NodeID) {
	b.parentID = id.(Uint16ID)
}

func (b *inMemoryBranch) LeftSiblingID() NodeID {
	return NodeID(b.leftID)
}

func (b *inMemoryBranch) SetLeftSiblingID(id NodeID) {
	b.leftID = id.(Uint16ID)
}

func (b *inMemoryBranch) RightSiblingID() NodeID {
	return NodeID(b.rightID)
}

func (b *inMemoryBranch) SetRightSiblingID(id NodeID) {
	b.rightID = id.(Uint16ID)
}

func (b *inMemoryBranch) KeyAt(position int) Key {
	return b.entries[position].Key
}

func (b *inMemoryBranch) EntryAt(position int) BranchEntry {
	return b.entries[position]
}

func (b *inMemoryBranch) Append(key Key, gteNodeID NodeID) {
	entry := BranchEntry{
		Key:                           key,
		LowerThanKeyNodeID:            b.entries[len(b.entries)-1].LowerThanKeyNodeID,
		GreaterThanOrEqualToKeyNodeID: gteNodeID,
	}
	b.entries = append(b.entries, entry)
}

func (l *inMemoryBranch) DeleteAt(position int) BranchEntry {
	entry := l.entries[position]
	if position == len(l.entries)-1 {
		l.entries = l.entries[0:position]
	} else if position == 0 {
		l.entries = l.entries[1:]
	} else {
		l.entries[position+1].LowerThanKeyNodeID = l.entries[position-1].GreaterThanOrEqualToKeyNodeID
		l.entries = append(l.entries[:position], l.entries[position+1:]...)
	}
	return entry
}

func (b *inMemoryBranch) ReplaceKeyAt(position int, key Key) {
	b.entries[position].Key = key
}

func (b *inMemoryBranch) DeleteFrom(startPosition int) BranchEntries {
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

func (b *inMemoryBranch) All(iterator BranchEntriesIterator) error {
	for _, entry := range b.entries {
		iterator(entry)
	}
	return nil
}

func (b *inMemoryBranch) InsertAt(position int, key Key, greaterThanOrEqualToKeyNodeID NodeID) {
	entry := BranchEntry{
		Key: key,
		GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
	}

	if position < 0 {
		panic("IS THIS CORRECT?")
	} else if position == 0 {
		entry.LowerThanKeyNodeID = b.entries[0].LowerThanKeyNodeID
		b.entries[0].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
		b.entries = append(BranchEntries{entry}, b.entries...)
	} else if position == len(b.entries) {
		entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
		b.entries = append(b.entries, entry)
	} else {
		entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
		b.entries[position].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
		b.entries = append(b.entries, entry)
		copy(b.entries[position+1:], b.entries[position:])
		b.entries[position] = entry
	}
}

func (b *inMemoryBranch) Unshift(key Key, lowerThanKeyNodeID NodeID) {
	entry := BranchEntry{
		Key:                           key,
		LowerThanKeyNodeID:            lowerThanKeyNodeID,
		GreaterThanOrEqualToKeyNodeID: b.entries[0].LowerThanKeyNodeID,
	}
	b.entries = append(BranchEntries{entry}, b.entries...)
}
