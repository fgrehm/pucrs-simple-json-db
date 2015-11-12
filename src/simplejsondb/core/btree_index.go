package core

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"simplejsondb/dbio"
)

type BTreeIndex interface {
	Add(searchKey uint32, rowID RowID)
	Find(searchKey uint32) (RowID, error)
	Remove(searchKey uint32)
	All() []RowID
}

type bTreeIndex struct {
	buffer                             dbio.DataBuffer
	repo                               DataBlockRepository
	leafCapacity, halfLeafCapacity     uint16
	branchCapacity, halfBranchCapacity uint16
}

func NewBTreeIndex(buffer dbio.DataBuffer, dataBlockRepository DataBlockRepository, leafCapacity, branchCapacity uint16) BTreeIndex {
	return &bTreeIndex{
		buffer:             buffer,
		repo:               dataBlockRepository,
		leafCapacity:       leafCapacity,
		halfLeafCapacity:   leafCapacity / 2,
		branchCapacity:     branchCapacity,
		halfBranchCapacity: branchCapacity / 2,
	}
}

func (idx *bTreeIndex) Add(searchKey uint32, rowID RowID) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())
	log.Printf("==IDX_ADD_BEGIN rootBlockID=%d, searchKey=%d, rowID=%+v", root.DataBlockID(), searchKey, rowID)

	var leaf BTreeLeaf
	if leafRoot, isLeaf := root.(BTreeLeaf); isLeaf {
		leaf = leafRoot
	} else {
		branchRoot, _ := root.(BTreeBranch)
		leaf = idx.findLeafFromBranch(branchRoot, searchKey)
	}
	if leaf == nil {
		log.Fatalf("Don't know where to insert %d", searchKey)
	}
	idx.addToLeaf(leaf, searchKey, rowID)
	log.Printf("==IDX_ADD_END searchKey=%d", searchKey)
}

func (idx *bTreeIndex) Find(searchKey uint32) (RowID, error) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())
	log.Printf("==IDX_FIND_BEGIN rootBlockID=%d, searchKey=%d", root.DataBlockID(), searchKey)

	var (
		rowID RowID
		leaf  BTreeLeaf
	)
	if branchRoot, isBranch := root.(BTreeBranch); isBranch {
		log.Printf("IDX_FIND_ON_BRANCH blockID=%d", branchRoot.DataBlockID())
		leaf = idx.findLeafFromBranch(branchRoot, searchKey)
	} else {
		leaf, _ = root.(BTreeLeaf)
	}

	if leaf != nil {
		log.Printf("IDX_FIND_ON_LEAF_ROOT blockID=%d", leaf.DataBlockID())
		rowID = leaf.Find(searchKey).RowID
	}

	if rowID == (RowID{}) {
		log.Printf("==IDX_FIND_END searchKey=%d NOT FOUND", searchKey)
		return rowID, fmt.Errorf("Not found: %d", searchKey)
	}

	log.Printf("==IDX_FIND_END searchKey=%d, rowID=%+v", searchKey, rowID)
	return rowID, nil
}

func (idx *bTreeIndex) Remove(searchKey uint32) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())

	log.Printf("==IDX_REMOVE_BEGIN rootBlockID=%d, searchKey=%d", root.DataBlockID(), searchKey)

	if leaf, isLeaf := root.(BTreeLeaf); isLeaf {
		log.Printf("IDX_REMOVE_FROM_LEAF_ROOT blockID=%d, searchKey=%d", root.DataBlockID(), searchKey)
		leaf.Remove(searchKey)
		idx.buffer.MarkAsDirty(leaf.DataBlockID())
	} else {
		log.Printf("IDX_REMOVE_FROM_BRANCH_ROOT blockID=%d, searchKey=%d", root.DataBlockID(), searchKey)
		branchRoot, _ := root.(BTreeBranch)
		idx.removeStartingFromBranch(branchRoot, searchKey)
	}
	log.Printf("==IDX_REMOVE_END searchKey=%d", searchKey)
}

func (idx *bTreeIndex) All() []RowID {
	entries := []RowID{}
	controlBlock := idx.repo.ControlBlock()
	leafID := controlBlock.FirstLeaf()

	log.Printf("==IDX_ALL_BEGIN firstLeafID=%d", leafID)
	for leafID != 0 {
		leaf := idx.repo.BTreeLeaf(leafID)
		entries = append(entries, leaf.All()...)
		leafID = leaf.RightSibling()
		log.Printf("IDX_ALL_NEXT leafID=%d", leafID)
	}
	log.Printf("==IDX_ALL_END totalEntries=%d", len(entries))
	return entries
}

// Node manipulation ==========================================================

func (idx *bTreeIndex) addToLeaf(leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	if leaf.EntriesCount() == idx.leafCapacity {
		idx.handleLeafSplit(leaf, searchKey, rowID)
	} else {
		log.Printf("IDX_ADD_TO_LEAF blockID=%d", leaf.DataBlockID())
		leaf.Add(searchKey, rowID)
		idx.buffer.MarkAsDirty(leaf.DataBlockID())
	}
}

func (idx *bTreeIndex) handleLeafSplit(leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	log.Printf("IDX_LEAF_SPLIT blockID=%d, searchKey=%d", leaf.DataBlockID(), searchKey)
	blocksMap := &dataBlocksMap{idx.buffer}

	right := CreateBTreeLeaf(idx.allocateBlock(blocksMap))
	log.Printf("IDX_LEAF_SPLIT_NEW_LEAF newBlockID=%d", right.DataBlockID())
	right.Add(searchKey, rowID)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(leaf)
	// EXTRACT COMMON METHOD
	if parent == nil { // AKA split on root node
		parent = CreateBTreeBranch(idx.allocateBlock(blocksMap))
		log.Printf("IDX_SET_ROOT blockID=%d", parent.DataBlockID())
		controlBlock := idx.repo.ControlBlock()
		controlBlock.SetBTreeRootBlock(parent.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
	}

	log.Printf("IDX_LEAF_ALLOC leftID=%d, parentID=%d, rightID=%d", leaf.DataBlockID(), parent.DataBlockID(), right.DataBlockID())

	leaf.SetParent(parent)
	leaf.SetRightSibling(right)
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	right.SetParent(parent)
	right.SetLeftSibling(leaf)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	idx.addToBranch(blocksMap, searchKey, parent, leaf, right)
}

func (idx *bTreeIndex) addToBranch(blocksMap DataBlocksMap, searchKey uint32, branch BTreeBranch, left, right BTreeNode) {
	if branch.EntriesCount() < idx.branchCapacity {
		branch.Add(searchKey, left, right)
		idx.buffer.MarkAsDirty(branch.DataBlockID())
		return
	}

	log.Printf("IDX_BRANCH_SPLIT blockID=%d, searchKey=%d", branch.DataBlockID(), searchKey)

	parent := idx.parent(branch)
	// EXTRACT COMMON METHOD
	if parent == nil { // AKA split on root node
		parent = CreateBTreeBranch(idx.allocateBlock(blocksMap))
		log.Printf("IDX_SET_ROOT blockID=%d", parent.DataBlockID())
		controlBlock := idx.repo.ControlBlock()
		controlBlock.SetBTreeRootBlock(parent.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
		branch.SetParent(parent)
	}

	rightBranchSibling := CreateBTreeBranch(idx.allocateBlock(blocksMap))
	log.Printf("IDX_BRANCH_ALLOC leftID=%d, parentID=%d, rightID=%d", branch.DataBlockID(), parent.DataBlockID(), rightBranchSibling.DataBlockID())

	rightBranchSibling.SetLeftSibling(branch)
	rightBranchSibling.Add(searchKey, left, right)
	rightBranchSibling.SetParent(parent)
	idx.buffer.MarkAsDirty(rightBranchSibling.DataBlockID())

	left.SetParent(rightBranchSibling)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	right.SetParent(rightBranchSibling)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parentSearchKey := branch.Pop().SearchKey
	branch.SetRightSibling(rightBranchSibling)
	idx.buffer.MarkAsDirty(branch.DataBlockID())

	idx.addToBranch(blocksMap, parentSearchKey, parent, branch, rightBranchSibling)
}

func (idx *bTreeIndex) removeStartingFromBranch(branchNode BTreeBranch, searchKey uint32) {
	leaf := idx.findLeafFromBranch(branchNode, searchKey)
	leaf.Remove(searchKey)
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	entriesCount := leaf.EntriesCount()
	log.Printf("IDX_REMOVED_FROM_LEAF blockID=%d, searchKey=%d, newEntriesCount=%d", leaf.DataBlockID(), searchKey, entriesCount)

	// Do we need to worry about moving keys around?
	if entriesCount >= idx.halfLeafCapacity {
		return
	}

	// Did we get to the right most leaf?
	right := idx.rightLeafSibling(leaf)
	if right == nil {
		idx.handleRemoveOnRightMostLeaf(searchKey, leaf)
		return
	}

	// Can we "borrow" a key from the right sibling instead of merging?
	rightNodeEntriesCount := right.EntriesCount()
	if rightNodeEntriesCount > idx.halfLeafCapacity {
		idx.pipeLeaf(leaf, right)
		return
	}

	// Yes, we need to merge nodes
	log.Printf("IDX_MERGE_LEAVES left=%d, right=%d", leaf.DataBlockID(), right.DataBlockID())
	idx.mergeLeaves(leaf, right)
}

func (idx *bTreeIndex) handleRemoveOnRightMostLeaf(removedKey uint32, leaf BTreeLeaf) {
	// We keep non empty right most leaf node around while they have at least
	// one entry since new records will be added here
	if leaf.EntriesCount() > 0 {
		return
	}

	// If the node has zeroed out, we need to free it up and update related nodes
	left := idx.leftLeafSibling(leaf)
	if left == nil {
		panic("Something weird happened")
	}
	left.SetRightSiblingID(uint16(0))
	idx.buffer.MarkAsDirty(left.DataBlockID())

	parent := idx.parent(leaf)
	idx.removeFromBranch(parent, removedKey, left)

	log.Printf("IDX_REMOVE_LAST_LEAF blockID=%d", leaf.DataBlockID())
	leaf.Reset()
	blocksMap := &dataBlocksMap{idx.buffer}
	blocksMap.MarkAsFree(leaf.DataBlockID())
	idx.buffer.MarkAsDirty(leaf.DataBlockID())
}

func (idx *bTreeIndex) pipeLeaf(left, right BTreeLeaf) {
	rowID := right.Shift()
	idx.buffer.MarkAsDirty(right.DataBlockID())

	log.Printf("IDX_PIPE_LEAF key=%d, from=%d, to=%d", rowID.RecordID, right.DataBlockID(), left.DataBlockID())

	left.Add(rowID.RecordID, rowID)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	parent := idx.parent(left)
	parent.ReplaceKey(rowID.RecordID, right.First().RecordID)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) mergeLeaves(left, right BTreeLeaf) {
	rightEntries := right.All()
	for _, entry := range rightEntries {
		left.Add(entry.RecordID, entry)
	}
	idx.buffer.MarkAsDirty(left.DataBlockID())

	if newRight := idx.rightLeafSibling(right); newRight != nil {
		log.Printf("IDX_MERGE_LEAVES_REMOVE_GAP left=%d, right=%d, newRight=%d", left.DataBlockID(), right.DataBlockID(), newRight.DataBlockID())
		left.SetRightSibling(newRight)
		newRight.SetLeftSibling(left)
		idx.buffer.MarkAsDirty(newRight.DataBlockID())
	}

	right.Reset()
	dataBlocksMap := &dataBlocksMap{idx.buffer}
	dataBlocksMap.MarkAsFree(right.DataBlockID())
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(left)
	idx.removeFromBranch(parent, rightEntries[0].RecordID, left)
}

func (idx *bTreeIndex) removeFromBranch(branch BTreeBranch, searchKey uint32, child BTreeNode) {
	branch.Remove(searchKey)
	idx.buffer.MarkAsDirty(branch.DataBlockID())

	if branch.EntriesCount() >= idx.halfBranchCapacity {
		return
	}

	if branch.IsRoot() {
		if branch.EntriesCount() == 0 {
			// EXTRACT COMMON METHOD
			log.Printf("IDX_SET_ROOT blockID=%d", child.DataBlockID())

			child.SetParentID(0)
			child.SetRightSiblingID(0)
			idx.buffer.MarkAsDirty(child.DataBlockID())

			branch.Reset()
			dataBlocksMap := &dataBlocksMap{idx.buffer}
			dataBlocksMap.MarkAsFree(branch.DataBlockID())

			controlBlock := idx.repo.ControlBlock()
			controlBlock.SetBTreeRootBlock(child.DataBlockID())
			idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
		}
		return
	}

	// Did we get to the right most branch?
	right := idx.rightBranchSibling(branch)
	if right == nil {
		idx.handleRemoveOnRightMostBranch(branch, searchKey, child)
		return
	}

	// Can we "borrow" a key from the right sibling instead of merging?
	rightNodeEntriesCount := right.EntriesCount()
	if rightNodeEntriesCount > idx.halfLeafCapacity {
		idx.pipeBranch(branch, right)
		return
	}

	idx.mergeBranches(branch, right)
}

func (idx *bTreeIndex) handleRemoveOnRightMostBranch(branch BTreeBranch, searchKey uint32, child BTreeNode) {
	panic("CANT REMOVE ON RIGHT MOST BRANCH YET")
}

func (idx *bTreeIndex) pipeBranch(left, right BTreeBranch) {
	entry := right.Shift()
	idx.buffer.MarkAsDirty(right.DataBlockID())

	log.Printf("IDX_PIPE_BRANCH key=%d, from=%d, to=%d", entry.SearchKey, right.DataBlockID(), left.DataBlockID())

	rightLowerThanChild := idx.repo.BTreeNode(entry.LtBlockID)
	newLeftKey := idx.firstKey(rightLowerThanChild)
	left.Append(newLeftKey, rightLowerThanChild)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	rightGreaterThanOrEqChild := idx.repo.BTreeNode(entry.GteBlockID)
	newParentKey := idx.firstKey(rightGreaterThanOrEqChild)

	parent := idx.parent(left)
	parent.ReplaceKey(entry.SearchKey, newParentKey)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) mergeBranches(left, right BTreeBranch) {
	rightEntries := right.All()
	for _, entry := range rightEntries {
		node := idx.repo.BTreeNode(entry.GteBlockID)
		left.Append(entry.SearchKey, node)
	}
	idx.buffer.MarkAsDirty(left.DataBlockID())

	if newRight := idx.rightBranchSibling(right); newRight != nil {
		log.Printf("IDX_MERGE_BRANCHES_REMOVE_GAP left=%d, right=%d, newRight=%d", left.DataBlockID(), right.DataBlockID(), newRight.DataBlockID())
		left.SetRightSibling(newRight)
		newRight.SetLeftSibling(left)
		idx.buffer.MarkAsDirty(newRight.DataBlockID())
	}

	right.Reset()
	dataBlocksMap := &dataBlocksMap{idx.buffer}
	dataBlocksMap.MarkAsFree(right.DataBlockID())
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(left)
	idx.removeFromBranch(parent, rightEntries[0].SearchKey, left)
}

func (idx *bTreeIndex) allocateBlock(blocksMap DataBlocksMap) *dbio.DataBlock {
	blockID := blocksMap.FirstFree()
	block, err := idx.buffer.FetchBlock(blockID)
	if err != nil {
		log.Panic(err)
	}
	blocksMap.MarkAsUsed(blockID)
	return block
}

// Tree traversal =============================================================

func (idx *bTreeIndex) firstKey(node BTreeNode) uint32 {
	// HACK: I'm not sure this is the best way of handling this but it
	// does the job for now
	if leaf, isLeaf := node.(BTreeLeaf); isLeaf {
		return leaf.First().RecordID

	} else if branch, isBranch := node.(BTreeBranch); isBranch {
		return branch.FirstEntry().SearchKey

	} else {
		panic("An unknown type of node was used o_O")
	}
}

func (idx *bTreeIndex) parent(node BTreeNode) BTreeBranch {
	if parentID := node.Parent(); parentID != 0 {
		return idx.repo.BTreeBranch(parentID)
	}
	return nil
}

func (idx *bTreeIndex) leftLeafSibling(leafNode BTreeLeaf) BTreeLeaf {
	if leftID := leafNode.LeftSibling(); leftID != 0 {
		return idx.repo.BTreeLeaf(leftID)
	}
	return nil
}

func (idx *bTreeIndex) rightLeafSibling(leafNode BTreeLeaf) BTreeLeaf {
	if rightID := leafNode.RightSibling(); rightID != 0 {
		return idx.repo.BTreeLeaf(rightID)
	}
	return nil
}

func (idx *bTreeIndex) rightBranchSibling(branchNode BTreeBranch) BTreeBranch {
	if rightID := branchNode.RightSibling(); rightID != 0 {
		return idx.repo.BTreeBranch(rightID)
	}
	return nil
}

func (idx *bTreeIndex) leftBranchSibling(branchNode BTreeBranch) BTreeBranch {
	if leftID := branchNode.RightSibling(); leftID != 0 {
		return idx.repo.BTreeBranch(leftID)
	}
	return nil
}

func (idx *bTreeIndex) findLeafFromBranch(branchNode BTreeBranch, searchKey uint32) BTreeLeaf {
	leafCandidateID := branchNode.Find(searchKey)
	for ; leafCandidateID != 0; leafCandidateID = branchNode.Find(searchKey) {
		leafCandidate := idx.repo.BTreeNode(leafCandidateID)
		if leaf, isLeaf := leafCandidate.(BTreeLeaf); isLeaf {
			return leaf
		} else {
			branchNode, _ = leafCandidate.(BTreeBranch)
		}
	}
	return nil
}
