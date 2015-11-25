package bplustree

import (
	"fmt"
	"sort"
)

func New(config Config) BPlusTree {
	return &bPlusTree{
		adapter:            config.Adapter,
		leafCapacity:       config.LeafCapacity,
		halfLeafCapacity:   config.LeafCapacity / 2,
		branchCapacity:     config.BranchCapacity,
		halfBranchCapacity: config.BranchCapacity / 2,
	}
}

type bPlusTree struct {
	adapter                            NodeAdapter
	leafCapacity, halfLeafCapacity     int
	branchCapacity, halfBranchCapacity int
}

type Config struct {
	Adapter        NodeAdapter
	LeafCapacity   int
	BranchCapacity int
}

func (t *bPlusTree) root() Node {
	return t.adapter.LoadRoot()
}

func (t *bPlusTree) Init() {
	t.adapter.Init()
}

func (t *bPlusTree) Insert(key Key, item Item) error {
	var leaf LeafNode
	root := t.adapter.LoadRoot()

	if root == nil {
		leaf = t.adapter.Init()
	} else if leafRoot, isLeaf := root.(LeafNode); isLeaf {
		leaf = leafRoot
	} else {
		leaf = t.findLeafForKey(root.(BranchNode), key)
	}

	insertPosition, found := t.findOnNode(leaf, key)
	if found {
		return fmt.Errorf("Key already exists: %+v", key)
	}

	t.insertOnLeaf(leaf, insertPosition, LeafEntry{key, item})
	return nil
}

func (t *bPlusTree) Delete(key Key) error {
	var leaf LeafNode
	root := t.adapter.LoadRoot()

	if root == nil {
		return fmt.Errorf("Key not found: %+v", key)
	}

	if leafRoot, isLeaf := root.(LeafNode); isLeaf {
		leaf = leafRoot
	} else {
		leaf = t.findLeafForKey(root.(BranchNode), key)
	}

	deletePosition, found := t.findOnNode(leaf, key)
	if !found {
		return fmt.Errorf("Key not found: %+v", key)
	}

	t.deleteFromLeaf(leaf, deletePosition)
	return nil
}

func (t *bPlusTree) deleteFromLeaf(leaf LeafNode, position int) {
	leaf.DeleteAt(position)

	if leaf.TotalKeys() >= t.halfLeafCapacity || t.adapter.IsRoot(leaf) {
		return
	}

	// Try "borrowing" an item from the right
	right := t.rightLeafSibling(leaf)
	if right != nil && right.TotalKeys() > t.halfLeafCapacity {
		t.pipeFromRightLeaf(right, leaf)
		return
	}

	// Try "borrowing" an item from the left
	left := t.leftLeafSibling(leaf)
	if left != nil && left.TotalKeys() > t.halfLeafCapacity {
		t.pipeFromLeftLeaf(left, leaf)
		return
	}

	// At this point we need to merge leaves, just need to figure out which one
	if right != nil {
		t.mergeLeaves(leaf, right)
	} else if left != nil {
		t.mergeLeaves(left, leaf)
	} else {
		// This is unlikely to happen but who knows...
		panic("Something weird happened")
	}
}

func (t *bPlusTree) deleteKeyFromBranch(branch BranchNode, key Key) {
	deletePosition, found := t.findOnNode(branch, key)
	if !found {
		deletePosition -= 1
	}
	t.deleteFromBranch(branch, deletePosition)
}

func (t *bPlusTree) deleteFromBranch(branch BranchNode, position int) {
	if position == 0 {
		branch.Shift()
	} else {
		branch.DeleteAt(position)
	}

	if branch.TotalKeys() >= t.halfBranchCapacity || t.adapter.IsRoot(branch) {
		return
	}

	// Try "borrowing" an item from the right
	right := t.rightBranchSibling(branch)
	if right != nil && right.TotalKeys() > t.halfBranchCapacity {
		t.pipeFromRightBranch(right, branch)
		return
	}

	// Try "borrowing" an item from the left
	left := t.leftBranchSibling(branch)
	if left != nil && left.TotalKeys() > t.halfBranchCapacity {
		t.pipeFromLeftBranch(left, branch)
		return
	}

	// At this point we need to merge nodes, just need to figure out which one
	if right != nil {
		t.mergeBranches(branch, right)
	} else if left != nil {
		t.mergeBranches(left, branch)
	} else {
		// This is unlikely to happen but who knows...
		panic("Something weird happened")
	}
}

func (t *bPlusTree) mergeBranches(left, right BranchNode) {
	insertPosition := left.TotalKeys()

	entry := right.EntryAt(0)
	leftKey := t.findMinimum(right)
	left.InsertAt(insertPosition, leftKey, entry.LowerThanKeyNodeID)
	t.updateParentID(entry.LowerThanKeyNodeID, left.ID())
	insertPosition += 1

	for right.TotalKeys() > 0 {
		newLeftEntry := right.DeleteAt(0)
		left.InsertAt(insertPosition, newLeftEntry.Key, newLeftEntry.GreaterThanOrEqualToKeyNodeID)
		t.updateParentID(newLeftEntry.GreaterThanOrEqualToKeyNodeID, left.ID())
		insertPosition += 1
	}
	left.SetRightSiblingID(right.RightSiblingID())

	newRight := t.rightBranchSibling(right)
	if newRight != nil {
		newRight.SetLeftSiblingID(left.ID())
	}

	t.adapter.Free(right)

	parent := t.adapter.LoadBranch(left.ParentID())
	if t.adapter.IsRoot(parent) && parent.TotalKeys() == 1 {
		t.adapter.Free(parent)
		t.adapter.SetRoot(left)
		return
	}

	t.deleteKeyFromBranch(parent, leftKey)
}

func (t *bPlusTree) pipeFromRightBranch(right, left BranchNode) {
	parent := t.adapter.LoadBranch(right.ParentID())
	positionToReplaceOnParent, found := t.findOnNode(parent, right.KeyAt(0))
	if !found {
		// If we were not able to find it, it means the key is greater than the
		// corresponding key on the parent node, so we move back one spot
		positionToReplaceOnParent -= 1
	}

	leftKey := parent.KeyAt(positionToReplaceOnParent)
	firstFromRight := right.DeleteAt(0)
	left.InsertAt(left.TotalKeys(), leftKey, firstFromRight.LowerThanKeyNodeID)

	child := t.adapter.LoadNode(firstFromRight.LowerThanKeyNodeID)
	child.SetParentID(left.ID())

	parentKey := t.findMinimum(right)
	parent.ReplaceKeyAt(positionToReplaceOnParent, parentKey)
}

func (t *bPlusTree) pipeFromLeftBranch(left, right BranchNode) {
	parent := t.adapter.LoadBranch(right.ParentID())
	positionToReplaceOnParent, found := t.findOnNode(parent, right.KeyAt(0))
	if !found {
		// If we were not able to find it, it means the key is greater than the
		// corresponding key on the parent node, so we move back one spot
		positionToReplaceOnParent -= 1
	}
	rightKey := parent.KeyAt(positionToReplaceOnParent)

	positionToRemoveOnLeft := left.TotalKeys() - 1
	lastFromLeft := left.DeleteAt(positionToRemoveOnLeft)
	right.Unshift(rightKey, lastFromLeft.GreaterThanOrEqualToKeyNodeID)

	child := t.adapter.LoadNode(lastFromLeft.GreaterThanOrEqualToKeyNodeID)
	child.SetParentID(right.ID())

	parent.ReplaceKeyAt(positionToReplaceOnParent, lastFromLeft.Key)

}

func (t *bPlusTree) rightBranchSibling(left BranchNode) BranchNode {
	right := t.adapter.LoadBranch(left.RightSiblingID())
	if right == nil || left.ParentID() != right.ParentID() {
		return nil
	}
	return right
}

func (t *bPlusTree) leftBranchSibling(right BranchNode) BranchNode {
	left := t.adapter.LoadBranch(right.LeftSiblingID())
	if left == nil || right.ParentID() != left.ParentID() {
		return nil
	}
	return left
}

func (t *bPlusTree) rightLeafSibling(left LeafNode) LeafNode {
	right := t.adapter.LoadLeaf(left.RightSiblingID())
	if right == nil || !left.ParentID().Equals(right.ParentID()) {
		return nil
	}
	return right
}

func (t *bPlusTree) leftLeafSibling(right LeafNode) LeafNode {
	left := t.adapter.LoadLeaf(right.LeftSiblingID())
	if left == nil || !right.ParentID().Equals(left.ParentID()) {
		return nil
	}
	return left
}

func (t *bPlusTree) pipeFromRightLeaf(right, left LeafNode) {
	firstFromRight := right.DeleteAt(0)
	left.InsertAt(left.TotalKeys(), firstFromRight)

	parent := t.adapter.LoadBranch(right.ParentID())
	position, found := t.findOnNode(parent, firstFromRight.Key)
	if !found {
		position -= 1
	}
	parent.ReplaceKeyAt(position, right.KeyAt(0))
}

func (t *bPlusTree) pipeFromLeftLeaf(left, right LeafNode) {
	firstFromRight := right.KeyAt(0)

	lastFromLeft := left.DeleteAt(left.TotalKeys() - 1)
	right.InsertAt(0, lastFromLeft)

	parent := t.adapter.LoadBranch(right.ParentID())
	position, found := t.findOnNode(parent, firstFromRight)
	if !found {
		position -= 1
	}
	parent.ReplaceKeyAt(position, lastFromLeft.Key)
}

func (t *bPlusTree) mergeLeaves(left, right LeafNode) {
	insertPosition := left.TotalKeys()
	right.All(func(entry LeafEntry) {
		left.InsertAt(insertPosition, entry)
		insertPosition += 1
	})

	newRight := t.adapter.LoadLeaf(right.RightSiblingID())
	if newRight != nil {
		newRight.SetLeftSiblingID(left.ID())
	}
	left.SetRightSiblingID(right.RightSiblingID())

	parentKeyCandidate := right.KeyAt(0)
	t.adapter.Free(right)

	parent := t.adapter.LoadBranch(left.ParentID())
	if t.adapter.IsRoot(parent) && parent.TotalKeys() == 1 {
		t.adapter.Free(parent)
		t.adapter.SetRoot(left)
		return
	}

	t.deleteKeyFromBranch(parent, parentKeyCandidate)
}

func (t *bPlusTree) Find(key Key) (Item, error) {
	var leaf LeafNode
	root := t.adapter.LoadRoot()

	if root == nil {
		return nil, fmt.Errorf("Key not found: %+v", key)
	}

	if leafRoot, isLeaf := root.(LeafNode); isLeaf {
		leaf = leafRoot
	} else {
		leaf = t.findLeafForKey(root.(BranchNode), key)
	}

	index, found := t.findOnNode(leaf, key)
	if !found {
		return nil, fmt.Errorf("Key not found: %+v", key)
	}

	return leaf.ItemAt(index), nil
}

func (t *bPlusTree) All(iterator LeafEntriesIterator) error {
	leaf := t.adapter.LoadFirstLeaf()
	for leaf != nil {
		rightID := leaf.RightSiblingID()
		if err := leaf.All(iterator); err != nil {
			return err
		}
		leaf = t.adapter.LoadLeaf(rightID)
	}
	return nil
}

func (t *bPlusTree) findOnNode(node Node, key Key) (int, bool) {
	totalKeys := node.TotalKeys()
	insertPosition := sort.Search(totalKeys, func(i int) bool {
		return !node.KeyAt(i).Less(key)
	})
	if insertPosition < totalKeys && node.KeyAt(insertPosition) == key {
		return insertPosition, true
	}
	return insertPosition, false
}

func (t *bPlusTree) findLeafForKey(node Node, key Key) LeafNode {
	for {
		if leaf, isLeaf := node.(LeafNode); isLeaf {
			return leaf
		}

		position, _ := t.findOnNode(node, key)
		if position >= node.TotalKeys() {
			position -= 1
		}
		entry := node.(BranchNode).EntryAt(position)
		childID := entry.GreaterThanOrEqualToKeyNodeID
		if key.Less(entry.Key) {
			childID = entry.LowerThanKeyNodeID
		}
		node = t.adapter.LoadNode(childID)
	}
}

func (t *bPlusTree) insertOnLeaf(leaf LeafNode, position int, entry LeafEntry) {
	totalKeys := leaf.TotalKeys()
	if totalKeys < t.leafCapacity {
		leaf.InsertAt(position, entry)
		return
	}
	right := t.leafSplit(leaf, position, entry)
	parentKey := right.KeyAt(0)
	if t.adapter.IsRoot(leaf) {
		t.allocateNewRoot(parentKey, leaf, right)
	} else {
		parent := t.adapter.LoadBranch(leaf.ParentID())
		insertPosition, found := t.findOnNode(parent, parentKey)
		if found {
			panic("Tried to insert a duplicate key on a branch")
		}
		right.SetParentID(parent.ID())
		t.insertOnBranch(parent, insertPosition, parentKey, right)
	}
}

func (t *bPlusTree) leafSplit(leaf LeafNode, position int, entry LeafEntry) LeafNode {
	splitFrom := t.halfLeafCapacity
	if position < splitFrom {
		splitFrom -= 1
	}

	rightEntries := leaf.DeleteFrom(splitFrom)

	right := t.adapter.CreateLeaf()
	for i, entry := range rightEntries {
		right.InsertAt(i, entry)
	}

	if position < t.halfLeafCapacity {
		leaf.InsertAt(position, entry)
	} else {
		right.InsertAt(position-t.halfLeafCapacity, entry)
	}
	t.setSiblings(leaf, right)

	return right
}

func (t *bPlusTree) insertOnBranch(branch BranchNode, position int, key Key, greaterThanOrEqToKeyNode Node) {
	if branch.TotalKeys() < t.branchCapacity {
		branch.InsertAt(position, key, greaterThanOrEqToKeyNode.ID())
		return
	}

	right, parentKey := t.branchSplit(branch, position, key, greaterThanOrEqToKeyNode)
	if t.adapter.IsRoot(branch) {
		t.allocateNewRoot(parentKey, branch, right)
		return
	}
	parent := t.adapter.LoadBranch(branch.ParentID())
	right.SetParentID(parent.ID())

	insertPosition, found := t.findOnNode(parent, parentKey)
	if found {
		panic("Attempted to add a key that already exists on a branch")
	}
	t.insertOnBranch(parent, insertPosition, parentKey, right)
}

func (t *bPlusTree) branchSplit(branch BranchNode, position int, key Key, greaterThanOrEqToKeyNode Node) (BranchNode, Key) {
	splitFrom := t.halfBranchCapacity
	if position < splitFrom {
		splitFrom -= 1
	}
	rightEntries := branch.DeleteFrom(splitFrom)

	if position == -1 {
		panic("Something weird is going on")
	} else if position < t.halfBranchCapacity {
		branch.InsertAt(position, key, greaterThanOrEqToKeyNode.ID())
	}

	if position == t.halfBranchCapacity {
		rightEntries[0].LowerThanKeyNodeID = greaterThanOrEqToKeyNode.ID()
	} else {
		rightEntries = rightEntries[1:]
	}

	right := t.adapter.CreateBranch(rightEntries[0])
	for i, entry := range rightEntries[1:] {
		right.InsertAt(i+1, entry.Key, entry.GreaterThanOrEqualToKeyNodeID)
	}

	if position > t.halfBranchCapacity {
		right.InsertAt(position-t.halfBranchCapacity-1, key, greaterThanOrEqToKeyNode.ID())
	}

	t.setSiblings(branch, right)
	t.updateParentID(right.EntryAt(0).LowerThanKeyNodeID, right.ID())
	right.All(func(entry BranchEntry) {
		t.updateParentID(entry.GreaterThanOrEqualToKeyNodeID, right.ID())
	})

	parentKey := t.findMinimum(right)
	return right, parentKey
}

func (t *bPlusTree) findMinimum(branch BranchNode) Key {
	var leaf LeafNode
	isLeaf := false
	for !isLeaf {
		node := t.adapter.LoadNode(branch.EntryAt(0).LowerThanKeyNodeID)
		leaf, isLeaf = node.(LeafNode)
		if !isLeaf {
			branch = node.(BranchNode)
		}
	}
	return leaf.KeyAt(0)
}

func (t *bPlusTree) findMaximum(branch BranchNode) Key {
	var leaf LeafNode
	isLeaf := false
	for !isLeaf {
		position := branch.TotalKeys() - 1
		node := t.adapter.LoadNode(branch.EntryAt(position).LowerThanKeyNodeID)
		leaf, isLeaf = node.(LeafNode)
		if !isLeaf {
			branch = node.(BranchNode)
		}
	}
	return leaf.KeyAt(leaf.TotalKeys() - 1)
}

func (t *bPlusTree) setSiblings(left, right Node) {
	oldRight := t.adapter.LoadNode(left.RightSiblingID())
	if oldRight != nil {
		oldRight.SetLeftSiblingID(right.ID())
		right.SetRightSiblingID(oldRight.ID())
	}

	left.SetRightSiblingID(right.ID())
	right.SetLeftSiblingID(left.ID())
}

func (t *bPlusTree) allocateNewRoot(key Key, ltNode, gteNode Node) BranchNode {
	entry := BranchEntry{
		Key:                           key,
		LowerThanKeyNodeID:            ltNode.ID(),
		GreaterThanOrEqualToKeyNodeID: gteNode.ID(),
	}
	parent := t.adapter.CreateBranch(entry)
	ltNode.SetParentID(parent.ID())
	gteNode.SetParentID(parent.ID())
	t.adapter.SetRoot(parent)
	return parent
}

func (t *bPlusTree) updateParentID(nodeID NodeID, newParentID NodeID) {
	node := t.adapter.LoadNode(nodeID)
	if node != nil {
		node.SetParentID(newParentID)
	}
}
