package bplustree

type BPlusTree interface {
	root() Node
	Insert(key Key, item Item) error
	Find(key Key) (Item, error)
	All(iterator LeafEntriesIterator) error
	Delete(key Key) error
}

type Key interface {
	Less(other Key) bool
}
type Item interface{}

type Node interface {
	ID() NodeID
	ParentID() NodeID
	SetParentID(id NodeID)
	SetLeftSiblingID(id NodeID)
	LeftSiblingID() NodeID
	SetRightSiblingID(id NodeID)
	RightSiblingID() NodeID
	KeyAt(position int) Key
	TotalKeys() int
}
type NodeID interface{}

type LeafNode interface {
	Node
	InsertAt(position int, entry LeafEntry)
	ItemAt(position int) Item
	DeleteAt(position int) LeafEntry
	DeleteFrom(position int) LeafEntries
	All(iterator LeafEntriesIterator) error
}
type LeafEntries []LeafEntry
type LeafEntry struct {
	Key  Key
	Item Item
}
type LeafEntriesIterator func(LeafEntry)

type BranchNode interface {
	Node
	InsertAt(position int, key Key, greaterThanOrEqualToKeyNodeID NodeID)
	Unshift(key Key, lowerThanKeyNodeID NodeID)
	EntryAt(position int) BranchEntry
	DeleteAt(position int) BranchEntry
	DeleteFrom(position int) BranchEntries
	Shift()
	ReplaceKeyAt(position int, newKey Key)
	All(BranchEntriesIterator) error
}
type BranchEntries []BranchEntry
type BranchEntry struct {
	Key                           Key
	LowerThanKeyNodeID            NodeID
	GreaterThanOrEqualToKeyNodeID NodeID
}
type BranchEntriesIterator func(BranchEntry)

type NodeAdapter interface {
	LoadRoot() Node
	IsRoot(node Node) bool
	Init() LeafNode
	SetRoot(node Node)
	LoadNode(id NodeID) Node
	Free(node Node)
	CreateBranch(entry BranchEntry) BranchNode
	LoadBranch(id NodeID) BranchNode
	LoadFirstLeaf() LeafNode
	CreateLeaf() LeafNode
	LoadLeaf(id NodeID) LeafNode
}
