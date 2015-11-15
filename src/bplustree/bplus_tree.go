package bplustree

import (
	"errors"
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
		return errors.New("Key already exists")
	}

	t.insertOnLeaf(leaf, insertPosition, LeafEntry{key, item})
	return nil
}

func (t *bPlusTree) Delete(key Key) error {
	var leaf LeafNode
	root := t.adapter.LoadRoot()

	if root == nil {
		return errors.New("Not found")
	}

	if leafRoot, isLeaf := root.(LeafNode); isLeaf {
		leaf = leafRoot
	} else {
		leaf = t.findLeafForKey(root.(BranchNode), key)
	}

	deletePosition, found := t.findOnNode(leaf, key)
	if !found {
		return errors.New("Not found")
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
	var parentKeyCandidate Key
	if right != nil {
		left, parentKeyCandidate = t.mergeLeaves(leaf, right)
	} else if left != nil {
		left, parentKeyCandidate = t.mergeLeaves(left, leaf)
	} else {
		// This is unlikely to happen but who knows...
		panic("Something weird happened")
	}

	parent := t.adapter.LoadBranch(left.ParentID())
	if t.adapter.IsRoot(parent) && parent.TotalKeys() == 1 {
		t.adapter.Free(parent)
		t.adapter.SetRoot(left)
		return
	}

	deletePosition, _ := t.findOnNode(parent, parentKeyCandidate)
	t.deleteFromBranch(parent, deletePosition, parentKeyCandidate)
}

func (t *bPlusTree) deleteFromBranch(branch BranchNode, position int, key Key) {
	if position == 0 {
		branch.Shift()
	} else {
		if position == branch.TotalKeys() {
			position -= 1
		}
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
	var parentKeyCandidate Key
	if right != nil {
		left, parentKeyCandidate = t.mergeBranches(branch, right)
	} else if left != nil {
		left, parentKeyCandidate = t.mergeBranches(left, branch)
	} else {
		// This is unlikely to happen but who knows...
		panic("Something weird happened")
	}

	parent := t.adapter.LoadBranch(left.ParentID())
	if parent.TotalKeys() <= t.halfBranchCapacity {
		panic("Can't clear a level of the tree yet")
		return
	}

	// REFACTOR: The code below looks the stuff above o_O
	deletePosition, _ := t.findOnNode(parent, parentKeyCandidate)
	if deletePosition == 0 {
		parent.Shift()
	} else {
		if deletePosition == parent.TotalKeys() {
			deletePosition -= 1
		}
		parent.DeleteAt(deletePosition)
	}

	grandParent := t.adapter.LoadBranch(parent.ParentID())
	if grandParent != nil {
		panic("Delete key from grandparent")
	}
}

func (t *bPlusTree) mergeBranches(left, right BranchNode) (BranchNode, Key) {
	insertPosition := left.TotalKeys()

	entry := right.EntryAt(0)
	newLeftKeyNodeID := entry.LowerThanKeyNodeID
	leftKey := t.adapter.LoadNode(newLeftKeyNodeID).KeyAt(0)
	left.InsertAt(insertPosition, leftKey, entry.LowerThanKeyNodeID)
	child := t.adapter.LoadNode(entry.LowerThanKeyNodeID)
	child.SetParentID(left.ID())
	insertPosition += 1

	right.All(func (entry BranchEntry) {
		left.InsertAt(insertPosition, entry.Key, entry.GreaterThanOrEqualToKeyNodeID)
		child := t.adapter.LoadNode(entry.GreaterThanOrEqualToKeyNodeID)
		child.SetParentID(left.ID())
		insertPosition += 1
	})
	left.SetRightSiblingID(right.RightSiblingID())

	newRight := t.rightBranchSibling(right)
	if newRight != nil {
		newRight.SetLeftSiblingID(left.ID())
	}

	middleKey := right.KeyAt(0)
	for right.TotalKeys() > 0 {
		right.DeleteAt(0)
	}
	t.adapter.Free(right)

	return left, middleKey
}

func (t *bPlusTree) pipeFromRightBranch(right, left BranchNode) {
	firstFromRight := right.DeleteAt(0)
	parent := t.adapter.LoadBranch(right.ParentID())

	position, _ := t.findOnNode(parent, firstFromRight.Key)
	leftKey := parent.KeyAt(position-1)
	parent.ReplaceKeyAt(position-1, right.KeyAt(0))

	left.InsertAt(left.TotalKeys(), leftKey, firstFromRight.LowerThanKeyNodeID)

	child := t.adapter.LoadNode(firstFromRight.LowerThanKeyNodeID)
	child.SetParentID(left.ID())
}

func (t *bPlusTree) pipeFromLeftBranch(left, right BranchNode) {
	lastFromLeft := left.DeleteAt(left.TotalKeys()-1)
	parent := t.adapter.LoadBranch(right.ParentID())

	position, _ := t.findOnNode(parent, lastFromLeft.Key)
	parent.ReplaceKeyAt(position+1, lastFromLeft.Key)

	newRightKeyNodeID := right.EntryAt(0).LowerThanKeyNodeID
	rightKey := t.adapter.LoadNode(newRightKeyNodeID).KeyAt(0)
	right.Unshift(rightKey, lastFromLeft.GreaterThanOrEqualToKeyNodeID)

	child := t.adapter.LoadNode(lastFromLeft.GreaterThanOrEqualToKeyNodeID)
	child.SetParentID(right.ID())
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
	if right == nil || left.ParentID() != right.ParentID() {
		return nil
	}
	return right
}

func (t *bPlusTree) leftLeafSibling(right LeafNode) LeafNode {
	left := t.adapter.LoadLeaf(right.LeftSiblingID())
	if left == nil || right.ParentID() != left.ParentID() {
		return nil
	}
	return left
}

func (t *bPlusTree) pipeFromRightLeaf(right, left LeafNode) {
	firstFromRight := right.DeleteAt(0)
	left.InsertAt(left.TotalKeys(), firstFromRight)

	parent := t.adapter.LoadBranch(right.ParentID())
	position, _ := t.findOnNode(parent, firstFromRight.Key)
	parent.ReplaceKeyAt(position, right.KeyAt(0))
}

func (t *bPlusTree) pipeFromLeftLeaf(left, right LeafNode) {
	firstFromRight := right.KeyAt(0)

	lastFromLeft := left.DeleteAt(left.TotalKeys()-1)
	right.InsertAt(0, lastFromLeft)

	parent := t.adapter.LoadBranch(right.ParentID())
	position, _ := t.findOnNode(parent, firstFromRight)
	parent.ReplaceKeyAt(position-1, lastFromLeft.Key)
}

func (t *bPlusTree) mergeLeaves(left, right LeafNode) (LeafNode, Key) {
	insertPosition := left.TotalKeys()
	right.All(func (entry LeafEntry) {
		left.InsertAt(insertPosition, entry)
		insertPosition += 1
	})
	left.SetRightSiblingID(right.RightSiblingID())

	newRight := t.rightLeafSibling(right)
	if newRight != nil {
		newRight.SetLeftSiblingID(left.ID())
	}

	middleKey := right.KeyAt(0)
	t.adapter.Free(right)
	return left, middleKey
}

func (t *bPlusTree) Find(key Key) (Item, error) {
	var leaf LeafNode
	root := t.adapter.LoadRoot()

	if root == nil {
		return nil, errors.New("Not found")
	}

	if leafRoot, isLeaf := root.(LeafNode); isLeaf {
		leaf = leafRoot
	} else {
		leaf = t.findLeafForKey(root.(BranchNode), key)
	}

	index, found := t.findOnNode(leaf, key)
	if !found {
		return nil, errors.New("Not found")
	}

	return leaf.ItemAt(index), nil
}

func (t *bPlusTree) All(iterator LeafEntriesIterator) error {
	leaf := t.adapter.LoadFirstLeaf()
	for leaf != nil {
		if err := leaf.All(iterator); err != nil {
			return err
		}
		leaf = t.adapter.LoadLeaf(leaf.RightSiblingID())
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

	t.updateParentID(right.EntryAt(0).LowerThanKeyNodeID, right.ID())
	right.All(func(entry BranchEntry) {
		t.updateParentID(entry.GreaterThanOrEqualToKeyNodeID, right.ID())
	})
	t.setSiblings(branch, right)

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
	node.SetParentID(newParentID)
}
