package core_test

import (
	"fmt"
	"simplejsondb/core"
	"simplejsondb/dbio"

	utils "test_utils"
	"testing"
)

var testRepo core.DataBlockRepository

func TestBTreeIndex_LeafRootNode(t *testing.T) {
	branchCapacity := 10
	leafCapacity := 8
	index := createTestBTreeIndex(t, 1, 4, branchCapacity, leafCapacity)

	// Start by ensuring that we can't find an entry that does not exist
	if _, err := index.Find(9); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Fill block up to its limit and ensure we can read a root leaf that is
	// not completely full
	assertIndexCanAddAndFindN(t, index, leafCapacity)

	// Ensure we error when the block is full but the entry does not exist
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted
	assertIndexCanRemoveN(t, index, leafCapacity)

	// Ensure we can't load the records anymore
	assertIndexFindErrorN(t, index, leafCapacity)

	// Just as a sanity check, can we add everything again after the node has been
	// cleared?
	assertIndexCanAddAndFindN(t, index, leafCapacity)
}

func TestBTreeIndex_LeafRootSplitAndMergeBack(t *testing.T) {
	branchCapacity := 10
	leafCapacity := 8
	index := createTestBTreeIndex(t, 3, 5, branchCapacity, leafCapacity)

	// Fill block up to its limit plus one and ensure we can read RowIDs back from
	// the index
	assertIndexCanAddAndFindN(t, index, leafCapacity+1)

	// Ensure we error when the leaf root node have been split and an unknown
	// record has been asked
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted (AKA merge)
	assertIndexCanRemoveN(t, index, leafCapacity+1)

	// Ensure we can't load the records anymore
	assertIndexFindErrorN(t, index, leafCapacity+1)

	// Just as a sanity check, can we add everything again after the node has been
	// cleared and merged?
	assertIndexCanAddAndFindN(t, index, leafCapacity+1)
}

func TestBTreeIndex_BranchRootSplitOnLeavesAndMergeBack(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	index := createTestBTreeIndex(t, 9, 10, branchCapacity, leafCapacity)

	totalEntries := (branchCapacity + 1) * leafCapacity
	// Trigger lots of splits on leaf nodes attached to the root
	assertIndexCanAddAndFindN(t, index, totalEntries)

	// Ensure we error when an unknown record has been asked after all those splits
	if _, err := index.Find(uint32(totalEntries * 2)); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted and collapse the tree
	// into a leaf node again
	assertIndexCanRemoveN(t, index, totalEntries)
	// Ensure we can't load the records anymore
	assertIndexFindErrorN(t, index, totalEntries)

	// Ensure things got restored to a state that the tree can be used again
	assertIndexCanAddAndFindN(t, index, totalEntries)
	// Ensure we can deal with removing entries from right to left
	assertIndexCanRemoveReverseRange(t, index, 1, totalEntries)

	// What about removing chunks of keys
	assertIndexCanAddAndFindN(t, index, totalEntries)
	assertIndexCanRemoveReverseRange(t, index, totalEntries/4, totalEntries/2)
	assertIndexCanFindRange(t, index, 1, totalEntries/4-1)
	assertIndexCanFindRange(t, index, totalEntries/2+1, totalEntries)
}

func TestBTreeIndex_BranchRootSplitOnBranchesAndMergeBack(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	index := createTestBTreeIndex(t, 200, 21, branchCapacity, leafCapacity)

	totalEntries := (branchCapacity + 1) * (branchCapacity + 1 ) * leafCapacity
	// Trigger lots of splits on leaf nodes attached to the root
	assertIndexCanAddAndFindN(t, index, totalEntries)
	assertIndexCanFindRange(t, index, 1, totalEntries)

	// Ensure we error when an unknown record has been asked after all those splits
	if _, err := index.Find(uint32(totalEntries * 2)); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted and collapse the tree
	// into a leaf node again
	indexDebug(index)
	// assertIndexCanRemoveN(t, index, totalEntries)
	// log.SetLevel(log.WarnLevel)
	// Ensure we can't load the records anymore
	// assertIndexFindErrorN(t, index, totalEntries)

	// Ensure things got restored to a state that the tree can be used again
	// assertIndexCanAddAndFindN(t, index, totalEntries)
	// Ensure we can deal with removing entries from right to left
	// assertIndexCanRemoveReverseRange(t, index, 1, totalEntries)

	// // What about removing chunks of keys
	// assertIndexCanAddAndFindN(t, index, totalEntries)
	// assertIndexCanRemoveReverseRange(t, index, uint32(totalEntries)/4, uint32(totalEntries)/2)
	// assertIndexCanFindRange(t, index, 1, uint32(totalEntries)/4-1)
	// assertIndexCanFindRange(t, index, uint32(totalEntries)/2+1, uint32(totalEntries))
}

func createTestBTreeIndex(t *testing.T, totalUsableBlocks, bufferFrames, branchCapacity, leafCapacity int) core.BTreeIndex {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		nil,
	}
	for i := 0; i < totalUsableBlocks; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, bufferFrames)
	testRepo = core.NewDataBlockRepository(dataBuffer)

	controlBlock := testRepo.ControlBlock()
	controlBlock.Format()
	dataBuffer.MarkAsDirty(controlBlock.DataBlockID())
	rootBlock, err := dataBuffer.FetchBlock(controlBlock.BTreeRootBlock())
	if err != nil {
		t.Fatal(err)
	}
	indexRoot := core.CreateBTreeLeaf(rootBlock)
	dataBuffer.MarkAsDirty(indexRoot.DataBlockID())

	blockMap := testRepo.DataBlocksMap()
	for i := uint16(0); i < 5; i++ {
		blockMap.MarkAsUsed(i)
	}

	return core.NewBTreeIndex(dataBuffer, testRepo, uint16(leafCapacity), uint16(branchCapacity))
}

func assertIndexFindErrorN(t *testing.T, index core.BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		id := uint32(i + 1)
		rowID, err := index.Find(id)
		if err == nil {
			t.Fatalf("Did not return an error when finding the record %d: %+v", id, rowID)
		}
	}
}

func assertIndexCanAddAndFindN(t *testing.T, index core.BTreeIndex, totalRecords int) {
	expectedRowIDs := []core.RowID{}
	for i := 0; i < totalRecords; i++ {
		id := i + 1
		expectedRowID := indexInsert(index, id, i)

		rowID, err := index.Find(uint32(id))
		if err != nil {
			panic(fmt.Sprintf("Error while fetching %d: %s", id, err))
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong core.RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
		expectedRowIDs = append(expectedRowIDs, expectedRowID)
	}
	allRowIDs := index.All()
	if len(expectedRowIDs) != len(allRowIDs) {
		t.Fatalf("Invalid set of row IDs returned by the index, got %+v, expected %+v", allRowIDs, expectedRowIDs)
	}
	for i := 0; i < len(allRowIDs); i++ {
		if expectedRowIDs[i] != allRowIDs[i] {
			t.Fatalf("Found a difference on the list of row ids returned by the index at %i, got %+v, expected %+v", i, allRowIDs[i], expectedRowIDs[i])
		}
	}
}

func assertIndexCanFindRange(t *testing.T, index core.BTreeIndex, firstID, lastID int) {
	for id := firstID; id <= lastID; id++ {
		expectedRowID := core.RowID{
			RecordID:    uint32(id),
			DataBlockID: uint16((id - 1) % 10),
			LocalID:     uint16((id - 1) % 100),
		}

		rowID, err := index.Find(uint32(id))
		if err != nil {
			panic(fmt.Sprintf("Error while fetching %d: %s", id, err))
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong core.RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}
}

func indexInsertN(index core.BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		indexInsert(index, i+1, i)
	}
}

func indexInsert(index core.BTreeIndex, id int, position int) core.RowID {
	rowID := core.RowID{
		RecordID:    uint32(id),
		DataBlockID: uint16(position % 10),
		LocalID:     uint16(position % 100),
	}
	index.Add(uint32(id), rowID)
	return rowID
}

func assertIndexCanRemoveN(t *testing.T, index core.BTreeIndex, totalRecords int) {
	totalBefore := len(index.All())
	for i := 0; i < totalRecords; i++ {
		id := uint32(i + 1)
		index.Remove(id)
	}
	totalAfter := len(index.All())
	if totalBefore != totalAfter+totalRecords {
		t.Fatal("Invalid data on index!")
	}
}

func assertIndexCanRemoveRange(t *testing.T, index core.BTreeIndex, firstID, lastID uint32) {
	totalBefore := len(index.All())
	for id := firstID; id <= lastID; id++ {
		index.Remove(id)
	}
	totalAfter := len(index.All())
	if totalBefore != totalAfter+int(lastID-firstID)+1 {
		t.Fatal("Invalid data on index!")
	}
}

func assertIndexCanRemoveReverseRange(t *testing.T, index core.BTreeIndex, firstID, lastID int) {
	totalBefore := len(index.All())
	for id := lastID; id >= firstID; id-- {
		index.Remove(uint32(id))
	}
	totalAfter := len(index.All())
	if totalBefore != totalAfter+int(lastID-firstID)+1 {
		t.Fatalf("Invalid data on index! before=%d, after=%d, firstid=%d, lastid=%d", totalBefore, totalAfter, firstID, lastID)
	}
}

func indexDebug(index core.BTreeIndex) {
	controlBlock := testRepo.ControlBlock()
	root := testRepo.BTreeNode(controlBlock.BTreeRootBlock())

	fmt.Print(indexDebugNode(index, "", root))
}

func indexDebugNode(index core.BTreeIndex, indent string, node core.BTreeNode) string {
	if leafRoot, isLeaf := node.(core.BTreeLeaf); isLeaf {
		return indexDebugLeaf(index, indent, leafRoot)
	} else {
		branchRoot, _ := node.(core.BTreeBranch)
		return indexDebugBranch(index, indent, branchRoot)
	}
}

func indexDebugLeaf(index core.BTreeIndex, indent string, leaf core.BTreeLeaf) string {
	keys := []uint32{}
	for _, entry := range leaf.All() {
		keys = append(keys, entry.RecordID)
	}
	return fmt.Sprintf(indent + "LEAF %+v\n", keys)
}

func indexDebugBranch(index core.BTreeIndex, indent string, branch core.BTreeBranch) string {
	entries := branch.All()

	output := fmt.Sprintf(indent + "BRANCH\n")
	indent += " "
	for _, entry := range entries {
		ltNode := testRepo.BTreeNode(entry.LtBlockID)
		childIndent := fmt.Sprintf("%s [< %d]", indent, entry.SearchKey)
		output += indexDebugNode(index, childIndent, ltNode)
	}
	gteNode := testRepo.BTreeNode(entries[len(entries)-1].GteBlockID)
	childIndent := fmt.Sprintf("%s [>=%d]", indent, entries[len(entries)-1].SearchKey)
	output += indexDebugNode(index, childIndent, gteNode)
	return output
}

