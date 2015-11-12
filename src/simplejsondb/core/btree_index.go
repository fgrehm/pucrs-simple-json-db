package core

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sort"
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
		for _, entry := range leaf.All() {
			entries = append(entries, entry.RowID)
		}
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

	parent := idx.parent(leaf)
	if parent == nil { // AKA split on root node
		parent = idx.allocateRoot(blocksMap)
	}

	right := CreateBTreeLeaf(idx.allocateBlock(blocksMap))
	right.SetParent(parent)
	right.SetLeftSibling(leaf)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	leaf.SetParent(parent)
	leaf.SetRightSibling(right)
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	log.Printf("IDX_LEAF_SPLIT_ALLOC leftID=%d, parentID=%d, rightID=%d", leaf.DataBlockID(), parent.DataBlockID(), right.DataBlockID())

	entries := leaf.All()
	insertPosition := sort.Search(len(entries), func(i int) bool {
		return entries[i].SearchKey >= searchKey
	})
	newEntry := BTreeLeafEntry{ SearchKey: searchKey, RowID: rowID }
	if insertPosition == 0 {
		entries = append([]BTreeLeafEntry{newEntry}, entries...)
	} else {
		firstChunk, lastChunk := entries[0:insertPosition], entries[insertPosition:]
		entries = append(firstChunk, newEntry)
		entries = append(entries, lastChunk...)
	}

	rightEntries := entries[idx.halfLeafCapacity:]
	for _, entry := range rightEntries {
		if entry.SearchKey != searchKey {
			leaf.Remove(entry.SearchKey)
		}
		right.Add(entry.SearchKey, entry.RowID)
	}
	if uint16(insertPosition) < idx.halfLeafCapacity {
		leaf.Add(newEntry.SearchKey, newEntry.RowID)
	}

	idx.addToBranch(blocksMap, entries[idx.halfLeafCapacity].SearchKey, parent, leaf, right)
}

func (idx *bTreeIndex) addToBranch(blocksMap DataBlocksMap, searchKey uint32, branch BTreeBranch, left, right BTreeNode) {
	// HERE BE DRAGONS!
	if branch.EntriesCount() < idx.branchCapacity {
		branch.Add(searchKey, left, right)
		idx.buffer.MarkAsDirty(branch.DataBlockID())
		return
	}

	log.Printf("IDX_BRANCH_SPLIT blockID=%d, searchKey=%d", branch.DataBlockID(), searchKey)

	parent := idx.parent(branch)
	if parent == nil { // AKA split on root node
		parent = idx.allocateRoot(blocksMap)
		branch.SetParent(parent)
	}

	rightBranchSibling := CreateBTreeBranch(idx.allocateBlock(blocksMap))
	rightBranchSibling.SetParent(parent)
	idx.buffer.MarkAsDirty(rightBranchSibling.DataBlockID())

	branch.SetRightSibling(rightBranchSibling)
	rightBranchSibling.SetLeftSibling(branch)

	log.Printf("IDX_BRANCH_ALLOC leftID=%d, parentID=%d, rightID=%d", branch.DataBlockID(), parent.DataBlockID(), rightBranchSibling.DataBlockID())

	entries := branch.All()
	insertPosition := sort.Search(len(entries), func(i int) bool {
		return entries[i].SearchKey >= searchKey
	})
	newEntry := BTreeBranchEntry{SearchKey: searchKey, LtBlockID: left.DataBlockID(), GteBlockID: right.DataBlockID() }
	if insertPosition == 0 {
		entries = append([]BTreeBranchEntry{newEntry}, entries...)
	} else {
		firstChunk, lastChunk := entries[0:insertPosition], entries[insertPosition:]
		entries = append(firstChunk, newEntry)
		entries = append(entries, lastChunk...)
	}

	rightEntries := entries[idx.halfBranchCapacity+1:]
	for _, entry := range rightEntries {
		if entry.SearchKey != searchKey {
			branch.Remove(entry.SearchKey)
			idx.buffer.MarkAsDirty(branch.DataBlockID())
		}
		entryLeft := idx.repo.BTreeNode(entry.LtBlockID)
		entryLeft.SetParent(rightBranchSibling)
		idx.buffer.MarkAsDirty(entryLeft.DataBlockID())

		entryRight := idx.repo.BTreeNode(entry.GteBlockID)
		entryRight.SetParent(rightBranchSibling)
		idx.buffer.MarkAsDirty(entryRight.DataBlockID())

		rightBranchSibling.Add(entry.SearchKey, entryLeft, entryRight)
		idx.buffer.MarkAsDirty(rightBranchSibling.DataBlockID())
	}
	if uint16(insertPosition) < idx.halfBranchCapacity {
		branch.Add(newEntry.SearchKey, left, right)
		idx.buffer.MarkAsDirty(branch.DataBlockID())
	}
	branch.Remove(entries[idx.halfLeafCapacity+1].SearchKey)
	leftEntries := entries[:idx.halfBranchCapacity]
	for _, entry := range leftEntries {
		entryLeft := idx.repo.BTreeNode(entry.LtBlockID)
		entryLeft.SetParent(branch)
		idx.buffer.MarkAsDirty(entryLeft.DataBlockID())

		entryRight := idx.repo.BTreeNode(entry.GteBlockID)
		entryRight.SetParent(branch)
		idx.buffer.MarkAsDirty(entryRight.DataBlockID())
	}
	idx.addToBranch(blocksMap, entries[idx.halfBranchCapacity].SearchKey, parent, branch, rightBranchSibling)
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

	// Can we "borrow" a key from the right sibling instead of merging?
	right := idx.rightLeafSibling(leaf)
	if right != nil && right.EntriesCount() > idx.halfLeafCapacity {
		idx.pipeLeafRightToLeft(right, leaf)
		return
	}

	// Can we "borrow" a key from the left sibling instead of merging?
	left := idx.leftLeafSibling(leaf)
	if left != nil && left.EntriesCount() > idx.halfLeafCapacity {
		idx.pipeLeafLeftToRight(left, leaf)
		return
	}

	// Yes, we need to merge nodes
	if right != nil {
		log.Printf("IDX_MERGE_LEAVES left=%d, right=%d", leaf.DataBlockID(), right.DataBlockID())
		idx.mergeLeaves(leaf, right)
		return
	}
	if left != nil {
		log.Printf("IDX_MERGE_LEAVES left=%d, right=%d", left.DataBlockID(), leaf.DataBlockID())
		idx.mergeLeaves(left, leaf)
		return
	}
	panic("Got into a weird state")
}

func (idx *bTreeIndex) pipeLeafRightToLeft(right, left BTreeLeaf) {
	if right.Parent() != left.Parent() {
		panic("Can't pipe from different parents")
	}

	entry := right.Shift()
	idx.buffer.MarkAsDirty(right.DataBlockID())

	log.Printf("IDX_PIPE_LEAF key=%d, from=%d, to=%d", entry.SearchKey, right.DataBlockID(), left.DataBlockID())

	left.Add(entry.SearchKey, entry.RowID)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	parent := idx.parent(left)
	parent.ReplaceKey(entry.SearchKey, right.First().SearchKey)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) pipeLeafLeftToRight(left, right BTreeLeaf) {
	if right.Parent() != left.Parent() {
		panic("Can't pipe from different parents")
	}

	entry := left.Pop()
	idx.buffer.MarkAsDirty(left.DataBlockID())

	log.Printf("IDX_PIPE_LEAF key=%d, from=%d, to=%d", entry.SearchKey, right.DataBlockID(), left.DataBlockID())

	keyToReplace := right.First().SearchKey
	right.Add(entry.SearchKey, entry.RowID)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(right)
	parent.ReplaceKey(keyToReplace, entry.SearchKey)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) mergeLeaves(left, right BTreeLeaf) {
	rightEntries := right.All()
	for _, entry := range rightEntries {
		left.Add(entry.SearchKey, entry.RowID)
	}
	idx.buffer.MarkAsDirty(left.DataBlockID())

	if newRight := idx.rightLeafSibling(right); newRight != nil {
		log.Printf("IDX_MERGE_LEAVES_REMOVE_GAP left=%d, right=%d, newRight=%d", left.DataBlockID(), right.DataBlockID(), newRight.DataBlockID())
		left.SetRightSibling(newRight)
		idx.buffer.MarkAsDirty(left.DataBlockID())
		newRight.SetLeftSibling(left)
		idx.buffer.MarkAsDirty(newRight.DataBlockID())
	}

	right.Reset()
	dataBlocksMap := &dataBlocksMap{idx.buffer}
	dataBlocksMap.MarkAsFree(right.DataBlockID())
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(left)
	if parent.DataBlockID() != right.Parent() {
		log.Printf("IDX_MERGE_LEAVES_REMOVE_GAP left=%d, right=%d, newRight=%d", left.DataBlockID(), right.DataBlockID(), newRight.DataBlockID())
		panic(fmt.Sprintf("Trying to merge 'cousin' leaves leftParent=%d, rightParent=%d", left.Parent(), right.Parent()))
	}
	idx.removeFromBranch(parent, rightEntries[0].SearchKey, left)
}

func (idx *bTreeIndex) removeFromBranch(branch BTreeBranch, searchKey uint32, child BTreeNode) {
	branch.Remove(searchKey)
	idx.buffer.MarkAsDirty(branch.DataBlockID())

	log.Printf("ENTRIES COUNT, %d, block=%d", branch.EntriesCount(), branch.DataBlockID())
	if branch.EntriesCount() >= idx.halfBranchCapacity {
		return
	}

	if branch.IsRoot() {
		if branch.EntriesCount() == 0 {
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

	// Can we "borrow" a key from the right sibling instead of merging?
	right := idx.rightBranchSibling(branch)
	if right != nil {
		rightNodeEntriesCount := right.EntriesCount()
		if rightNodeEntriesCount > idx.halfLeafCapacity {
			idx.pipeBranch(branch, right)
			return
		}
	}

	// Can we "borrow" a key from the left sibling instead of merging?
	left := idx.leftBranchSibling(branch)
	if left != nil {
		leftNodeEntriesCount := left.EntriesCount()
		if leftNodeEntriesCount > idx.halfLeafCapacity {
			idx.pipeBranch(left, branch)
			return
		}
	}

	// We gotta merge, just need to figure out which way
	if right != nil {
		log.Printf("IDX_MERGE_BRANCHES left=%d, right=%d", branch.DataBlockID(), right.DataBlockID())
		idx.mergeBranches(branch, right)
	} else if left != nil {
		log.Printf("IDX_MERGE_BRANCHES left=%d, right=%d", left.DataBlockID(), branch.DataBlockID())
		idx.mergeBranches(left, branch)
	} else {
		log.Panicf("Don't know how to merge the branch %d, it has no siblings", branch.DataBlockID())
	}
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
	if right.Parent() != left.Parent() {
		log.Panic("Can't handle merge across parents")
	}

	rightEntries := right.All()
	for _, entry := range rightEntries {
		node := idx.repo.BTreeNode(entry.GteBlockID)
		left.Append(entry.SearchKey, node)
	}
	idx.buffer.MarkAsDirty(left.DataBlockID())

	if newRight := idx.rightBranchSibling(right); newRight != nil {
		log.Printf("IDX_MERGE_BRANCHES_REMOVE_GAP left=%d, right=%d, newRight=%d", left.DataBlockID(), right.DataBlockID(), newRight.DataBlockID())
		left.SetRightSibling(newRight)
		idx.buffer.MarkAsDirty(left.DataBlockID())
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

func (idx *bTreeIndex) allocateRoot(blocksMap DataBlocksMap) BTreeBranch {
	root := CreateBTreeBranch(idx.allocateBlock(blocksMap))
	controlBlock := idx.repo.ControlBlock()
	controlBlock.SetBTreeRootBlock(root.DataBlockID())
	idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
	log.Printf("IDX_NEW_ROOT blockID=%d", root.DataBlockID())
	return root
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
		return leaf.First().SearchKey

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
