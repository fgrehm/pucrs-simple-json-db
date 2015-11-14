package bplustree

type BPlusTree interface {
	root() Node
	Insert(key Key, item Item) error
	Find(key Key) (Item, error)
	All(iterator LeafEntriesIterator) error
	// DeleteAt(key Key) error
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
	EntryAt(position int) BranchEntry
	DeleteFrom(position int) BranchEntries
	// ReplaceKey(oldKey, newKey Key)
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
	CreateBranch(entry BranchEntry) BranchNode
	LoadBranch(id NodeID) BranchNode
	LoadFirstLeaf() LeafNode
	CreateLeaf() LeafNode
	LoadLeaf(id NodeID) LeafNode
}

// type BPlusTree interface {
// 	Delete(key Key)
// 	All(iterator ItemIterator)
// 	Height() int
// }
//
// type NodeAdapter interface {
// 	CreateBranch() BranchNode
// 	GetBranch(id NodeID) BranchNode
// 	CreateLeaf() LeafNode
// 	GetLeaf(id NodeID) LeafNode
// }
//
// type BranchNode interface {
// 	Node
// 	Append(key Key, gteChildID NodeID)
// 	Delete(key Key)
// 	ReplaceKey(oldKey, newKey Key)
// 	Search(key Key) BranchEntry
// 	All() []BranchEntry
// 	LeftSibling() BranchNode
// 	RightSibling() BranchNode
// }
//
// type BTreeBranchEntry struct {
// 	SearchKey                      Key
// 	LowerThanKeyChildID            NodeID
// 	GreaterThanOrEqualToKeyChildID NodeID
// }
//
// type BTreeBranchEntry struct {
// 	SearchKey Key
// 	Item      Item
// }
