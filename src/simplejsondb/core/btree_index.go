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
	idx.addToLeaf(controlBlock, leaf, searchKey, rowID)
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
		rowID = leaf.Find(searchKey)
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
		idx.removeFromBranch(controlBlock, branchRoot, searchKey)
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

func (idx *bTreeIndex) addToLeaf(controlBlock ControlBlock, leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	if leaf.EntriesCount() == idx.leafCapacity {
		idx.handleLeafSplit(controlBlock, leaf, searchKey, rowID)
	} else {
		log.Printf("IDX_ADD_TO_LEAF blockID=%d", leaf.DataBlockID())
		leaf.Add(searchKey, rowID)
		idx.buffer.MarkAsDirty(leaf.DataBlockID())
	}
}

func (idx *bTreeIndex) handleLeafSplit(controlBlock ControlBlock, leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	log.Printf("IDX_LEAF_SPLIT blockID=%d, searchKey=%d", leaf.DataBlockID(), searchKey)
	blocksMap := &dataBlocksMap{idx.buffer}

	right := CreateBTreeLeaf(idx.allocateBlock(blocksMap))
	log.Printf("IDX_LEAF_SPLIT_NEW_LEAF newBlockID=%d", right.DataBlockID())
	right.Add(searchKey, rowID)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(leaf)
	if parent == nil { // AKA split on root node
		parent = CreateBTreeBranch(idx.allocateBlock(blocksMap))
		log.Printf("IDX_SET_ROOT blockID=%d", parent.DataBlockID())
		controlBlock.SetBTreeRootBlock(parent.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
	}

	log.Printf("IDX_LEAF_ALLOC leftID=%d, parentID=%d, rightID=%d", leaf.DataBlockID(), parent.DataBlockID(), right.DataBlockID())

	// Add entry to the internal branch node
	if parent.EntriesCount() == idx.branchCapacity {
		panic("Can't split branch yet")
	}
	parent.Add(searchKey, leaf, right)

	// Update sibling pointers
	right.SetLeftSibling(leaf)
	leaf.SetRightSibling(right)

	// Set parent node pointers
	right.SetParent(parent)
	leaf.SetParent(parent)

	// Let data be persisted
	idx.buffer.MarkAsDirty(right.DataBlockID())
	idx.buffer.MarkAsDirty(parent.DataBlockID())
	idx.buffer.MarkAsDirty(leaf.DataBlockID())
}

func (idx *bTreeIndex) removeFromBranch(controlBlock ControlBlock, branchNode BTreeBranch, searchKey uint32) {
	leaf := idx.findLeafFromBranch(branchNode, searchKey)
	leaf.Remove(searchKey)
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	if !branchNode.IsRoot() {
		panic("Don't know what to do with a branch that is not the root node")
	}

	entriesCount := leaf.EntriesCount()
	log.Printf("IDX_REMOVE_FROM_LEAF blockID=%d, searchKey=%d, entriesCount=%d", leaf.DataBlockID(), searchKey, entriesCount)

	// Did we get to the right most leaf?
	right := idx.rightLeafSibling(leaf)
	if right == nil {
		idx.handleRemoveOnRightMostLeaf(controlBlock, searchKey, leaf)
		return
	}

	// Do we need to worry about moving keys around?
	if entriesCount >= idx.halfLeafCapacity {
		return
	}

	// Can we "borrow" a key from the right sibling instead of merging?
	rightNodeEntriesCount := right.EntriesCount()
	if rightNodeEntriesCount > idx.halfLeafCapacity {
		idx.pipeFirst(leaf, right)
		return
	}

	// Yes, we need to merge nodes
	log.Printf("IDX_MERGE_LEAVES left=%d, right=%d", leaf.DataBlockID(), right.DataBlockID())
	idx.mergeLeaves(controlBlock, leaf, right)
}

func (idx *bTreeIndex) pipeFirst(left, right BTreeLeaf) {
	rowID := right.Shift()
	idx.buffer.MarkAsDirty(right.DataBlockID())

	log.Printf("IDX_PIPE key=%d, from=%d, to=%d", rowID.RecordID, right.DataBlockID(), left.DataBlockID())

	left.Add(rowID.RecordID, rowID)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	parent := idx.parent(left)
	parent.ReplaceKey(rowID.RecordID, right.First().RecordID)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) mergeLeaves(controlBlock ControlBlock, left, right BTreeLeaf) {
	parent := idx.parent(left)
	if !parent.IsRoot() {
		panic("Don't know how to merge a leaf into a parent that is not the root node")
	}

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

	parent.Remove(rightEntries[0].RecordID)
	idx.buffer.MarkAsDirty(parent.DataBlockID())

	if parent.IsRoot() && parent.EntriesCount() == 0 {
		log.Printf("IDX_MERGE_LEAVES_SET_ROOT blockID=%d", left.DataBlockID())

		parent.Reset()
		dataBlocksMap.MarkAsFree(parent.DataBlockID())

		left.SetParentID(0)
		left.SetRightSiblingID(0)
		controlBlock.SetBTreeRootBlock(left.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
		return
	}

	if !parent.IsRoot() && parent.EntriesCount() < idx.branchCapacity {
		panic("Don't know how to cascade merges yet")
	}
}

func (idx *bTreeIndex) handleRemoveOnRightMostLeaf(controlBlock ControlBlock, removedKey uint32, leaf BTreeLeaf) {
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
	parent.Remove(removedKey)
	idx.buffer.MarkAsDirty(parent.DataBlockID())

	log.Printf("IDX_REMOVE_LAST_LEAF blockID=%d", leaf.DataBlockID())
	leaf.Reset()
	blocksMap := &dataBlocksMap{idx.buffer}
	blocksMap.MarkAsFree(leaf.DataBlockID())
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	if parent.IsRoot() && parent.EntriesCount() == 0 {
		log.Printf("IDX_SET_ROOT blockID=%d", left.DataBlockID())

		parent.Reset()
		blocksMap.MarkAsFree(parent.DataBlockID())

		left.SetParentID(0)
		left.SetRightSiblingID(0)
		controlBlock.SetBTreeRootBlock(left.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
		return
	}

	if !parent.IsRoot() && parent.EntriesCount() < idx.branchCapacity {
		panic("Don't know how to cascade merges yet")
	}
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
