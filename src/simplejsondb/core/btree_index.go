package core

import (
	"fmt"

	"simplejsondb/dbio"

	log "github.com/Sirupsen/logrus"
)

type BTreeIndex interface {
	Add(searchKey uint32, rowID RowID)
	Find(searchKey uint32) (RowID, error)
	Remove(searchKey uint32)
	All() []RowID
}

type bTreeIndex struct {
	buffer dbio.DataBuffer
	repo   DataBlockRepository
}

// NOTE: This assumes that search keys will be added in order
func (idx *bTreeIndex) Add(searchKey uint32, rowID RowID) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())

	if leafRoot, isLeaf := root.(BTreeLeaf); isLeaf {
		idx.addToLeaf(controlBlock, leafRoot, searchKey, rowID)
	} else {
		branchRoot, _ := root.(BTreeBranch)
		idx.addToBranchRoot(controlBlock, branchRoot, searchKey, rowID)
	}
}

func (idx *bTreeIndex) Find(searchKey uint32) (RowID, error) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())
	var rowID RowID

	log.Printf("INDEX_FIND rootblockid=%d, searchkey=%d", root.DataBlockID(), searchKey)

	if leaf, isLeaf := root.(BTreeLeaf); isLeaf {
		log.Printf("LEAF rootblockid=%d, searchkey=%d", root.DataBlockID(), searchKey)
		rowID = leaf.Find(searchKey)
	} else {
		log.Printf("BRANCH rootblockid=%d, searchkey=%d", root.DataBlockID(), searchKey)
		rootBranch, _ := root.(BTreeBranch)
		if leaf := idx.findLeafFromBranch(rootBranch, searchKey); leaf != nil {
			rowID = leaf.Find(searchKey)
		}
	}

	if rowID == (RowID{}) {
		return rowID, fmt.Errorf("Search key not found: %d", searchKey)
	}

	return rowID, nil
}

func (idx *bTreeIndex) Remove(searchKey uint32) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeNode(controlBlock.BTreeRootBlock())

	log.Printf("INDEX_REMOVE rootblockid=%d, searchkey=%d", root.DataBlockID(), searchKey)

	if leaf, isLeaf := root.(BTreeLeaf); isLeaf {
		leaf.Remove(searchKey)
		idx.buffer.MarkAsDirty(leaf.DataBlockID())
	} else {
		rootBranch, _ := root.(BTreeBranch)
		idx.removeFromBranch(controlBlock, rootBranch, searchKey)
	}
}

func (idx *bTreeIndex) All() []RowID {
	entries := []RowID{}
	controlBlock := idx.repo.ControlBlock()
	leafID := controlBlock.FirstLeaf()

	for leafID != 0 {
		leaf := idx.repo.BTreeLeaf(leafID)
		entries = append(entries, leaf.All()...)
		leafID = leaf.RightSibling()
		log.Printf("INDEX_ALL nextleafid=%d", leafID)
	}
	return entries
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

func (idx *bTreeIndex) addToBranchRoot(controlBlock ControlBlock, branchNode BTreeBranch, searchKey uint32, rowID RowID) {
	if branchNode.EntriesCount() == BTREE_BRANCH_MAX_ENTRIES {
		log.Panic("Can't split branch yet")
		// else if root is a branch and needs a split
		//   right := CreateBTreeBranch
		//   root.SetRightSibling(right.DataBlockID())
		//   right.SetLeftSibling(root.DataBlockID())
		//   newRoot := CreateBTreeBranch
		//   entries := root.All()
		//   // Since we always insert keys in order, we always append the record at the
		//   // end of the node
		//   // TODO: Add entries[entries/2+1:] to the right
		//   // TODO: Add entries[entries/2] to the new root
		//   // TODO: Remove entries[entries/2:] from root (reverse the list and remove from the end)
		//   newRoot.Add(middle.RecordID, root.DataBlockID(), right.DataBlockID())
		//   root.SetParent(newRoot.DataBlock())
		//   right.SetParent(newRoot.DataBlock())
		//   controlBlock.SetRootBTreeBlock(newRoot.DataBlockID())
	}

	leaf := idx.findLeafFromBranch(branchNode, searchKey)
	if leaf == nil {
		log.Fatalf("Don't know where to insert %d", searchKey)
	}
	idx.addToLeaf(controlBlock, leaf, searchKey, rowID)
}

func (idx *bTreeIndex) removeFromBranch(controlBlock ControlBlock, branchNode BTreeBranch, searchKey uint32) {
	leaf := idx.findLeafFromBranch(branchNode, searchKey)
	leaf.Remove(searchKey)
	idx.buffer.MarkAsDirty(leaf.DataBlockID())

	entriesCount := leaf.EntriesCount()

	if !branchNode.IsRoot() {
		log.Panic("Don't know what to do with a branch that is not the root node")
	}

	if entriesCount == 0 && leaf.RightSibling() == 0 {
		parent := idx.parent(leaf)
		parent.Remove(searchKey)
		idx.buffer.MarkAsDirty(parent.DataBlockID())

		left := idx.leftLeafSibling(leaf)
		left.SetRightSiblingID(0)
		idx.buffer.MarkAsDirty(left.DataBlockID())

		leaf.Reset()
		idx.buffer.MarkAsDirty(leaf.DataBlockID())

		dataBlocksMap := &dataBlocksMap{idx.buffer}
		dataBlocksMap.MarkAsFree(leaf.DataBlockID())
		return
	}

	if entriesCount == 0 {
		log.Panic("Don't know what to do with a zeroed leaf yet")
	}

	// Do we need to think about moving keys around?
	if entriesCount >= BTREE_LEAF_MAX_ENTRIES/2 {
		return
	}

	// Do we have a right sibling?
	right := idx.rightLeafSibling(leaf)
	if right == nil {
		return
	}

	// Can we "borrow" a key from the right sibling instead of merging?
	entriesCount = right.EntriesCount()
	if entriesCount > BTREE_LEAF_MAX_ENTRIES/2 {
		idx.pipeFirst(leaf, right)
		return
	}

	// Yes, we need to merge nodes
	log.Printf("MERGE_LEAVES left=%d, right=%d", leaf.DataBlockID(), right.DataBlockID())
	idx.mergeLeaves(controlBlock, leaf, right)
}

func (idx *bTreeIndex) mergeLeaves(controlBlock ControlBlock, left, right BTreeLeaf) {
	parent := idx.parent(left)
	if !parent.IsRoot() {
		log.Panic("Don't know how to merge a leaf into a parent that is not the root node")
	}

	rightEntries := right.All()
	for _, entry := range rightEntries {
		left.Add(entry.RecordID, entry)
	}
	if rightRightSibling := right.RightSibling(); rightRightSibling != 0 {
		newRight := idx.repo.BTreeLeaf(rightRightSibling)
		left.SetRightSibling(newRight)
		newRight.SetLeftSibling(left)
		idx.buffer.MarkAsDirty(newRight.DataBlockID())
	}
	idx.buffer.MarkAsDirty(left.DataBlockID())

	right.Reset()
	dataBlocksMap := &dataBlocksMap{idx.buffer}
	dataBlocksMap.MarkAsFree(right.DataBlockID())
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent.Remove(rightEntries[0].RecordID)
	idx.buffer.MarkAsDirty(parent.DataBlockID())

	if parent.IsRoot() && parent.EntriesCount() == 0 {
		parent.Reset()
		dataBlocksMap.MarkAsFree(parent.DataBlockID())

		left.SetParentID(0)
		controlBlock.SetBTreeRootBlock(left.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
		return
	}
	if !parent.IsRoot() && parent.EntriesCount() < BTREE_BRANCH_MAX_ENTRIES/2 {
		log.Panic("Don't know how to cascade merges yet")
	}
}

func (idx *bTreeIndex) pipeFirst(left, right BTreeLeaf) {
	rowID := right.Shift()
	idx.buffer.MarkAsDirty(right.DataBlockID())

	log.Printf("INDEX_PIPE left=%d, right=%d, key=%d", left.DataBlockID(), right.DataBlockID(), rowID.RecordID)

	left.Add(rowID.RecordID, rowID)
	idx.buffer.MarkAsDirty(left.DataBlockID())

	parent := idx.parent(left)
	parent.ReplaceKey(rowID.RecordID, right.First().RecordID)
	idx.buffer.MarkAsDirty(parent.DataBlockID())
}

func (idx *bTreeIndex) addToLeaf(controlBlock ControlBlock, leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	if leaf.IsFull() {
		log.Printf("INDEX_LEAF_SPLIT blockid=%d, searchkey=%d, rowid=%+v", leaf.DataBlockID(), searchKey, rowID)
		idx.handleLeafSplit(controlBlock, leaf, searchKey, rowID)
	} else {
		log.Printf("INDEX_ADD blockid=%d, searchkey=%d, rowid=%+v", leaf.DataBlockID(), searchKey, rowID)
		leaf.Add(searchKey, rowID)
		idx.buffer.MarkAsDirty(leaf.DataBlockID())
	}
}

func (idx *bTreeIndex) handleLeafSplit(controlBlock ControlBlock, leaf BTreeLeaf, searchKey uint32, rowID RowID) {
	blocksMap := &dataBlocksMap{idx.buffer}

	right := idx.allocateLeaf(blocksMap)
	log.Debugf("Right node of the leaf node will be set to %d", right.DataBlockID())
	right.Add(searchKey, rowID)
	idx.buffer.MarkAsDirty(right.DataBlockID())

	parent := idx.parent(leaf)
	if parent == nil { // AKA split on root node
		parent = idx.allocateBranch(blocksMap)
		log.Printf("SET_BTREE_ROOT datablockid=%d", parent.DataBlockID())
		controlBlock.SetBTreeRootBlock(parent.DataBlockID())
		idx.buffer.MarkAsDirty(controlBlock.DataBlockID())
	}

	log.Printf("HANDLE_SPLIT datablockid=%d, parent=%d, right=%d", leaf.DataBlockID(), parent.DataBlockID(), right.DataBlockID())

	// Add entry to the internal branch node
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

func (idx *bTreeIndex) allocateLeaf(blocksMap DataBlocksMap) BTreeLeaf {
	return CreateBTreeLeaf(idx.allocateBlock(blocksMap))
}

func (idx *bTreeIndex) allocateBranch(blocksMap DataBlocksMap) BTreeBranch {
	return CreateBTreeBranch(idx.allocateBlock(blocksMap))
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
