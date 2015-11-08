package core_test

import (
	"fmt"
	"simplejsondb/core"
	"simplejsondb/dbio"

	utils "test_utils"
	"testing"
)

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

	totalEntries := (branchCapacity+1) * leafCapacity
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
	assertIndexCanRemoveReverseRange(t, index, uint32(1), uint32(totalEntries))

	// What about removing chunks of keys
	// assertIndexCanRemoveReverseRange(t, index, uint32(totalEntries)/4, uint32(totalEntries)/2)
	// assertIndexCanFindRange(t, index, 1, uint32(totalEntries)/4-1)
	// assertIndexCanFindRange(t, index, uint32(totalEntries)/2+1, uint32(totalEntries))
	// log.SetLevel(log.WarnLevel)
	// assertIndexCanFindRange(t, index, uint33(totalEntries)/2-1, uint32(totalEntries))
	// assertIndexCanRemoveReverseRange(t, index, 4001, uint32(totalEntries)/2-1)
	// assertIndexCanRemoveReverseRange(t, index, 511, 1020)
	// assertIndexCanRemoveRange(t, index, 79, 510)
	// // assertIndexCanFindRange(t, index, 4000, 10000)
	// assertIndexCanRemoveRange(t, index, 1021, 4000)
	// assertIndexCanRemoveReverseRange(t, index, 4001, uint32(totalEntries)/2-1)
}

func TestBTreeIndex_BranchRootSplitOnLeavesAndMergeBack2(t *testing.T) {
	t.Fatal("TODO: Try this again after the bug has been fixed")
	// index := createTestBTreeIndex(t, core.BTREE_BRANCH_MAX_ENTRIES*1.15, 256)
	// totalEntries := core.BTREE_BRANCH_MAX_ENTRIES * core.BTREE_LEAF_MAX_ENTRIES

	// log.SetLevel(log.WarnLevel)
	// // Trigger lots of splits on leaf nodes attached to the root
	// assertIndexCanAddAndFindN(t, index, totalEntries)

	// // Ensure we error when an unknown record has been asked after all those splits
	// if _, err := index.Find(uint32(totalEntries * 2)); err == nil {
	// 	t.Fatal("Did not return an error when finding a record that does not exist")
	// }

	// // Remove all of the records we have just inserted (AKA merge)
	// assertIndexCanRemoveN(t, index, totalEntries)

	// // Ensure we can't load the records anymore
	// assertIndexFindErrorN(t, index, totalEntries)

	// // Just as a sanity check, can we add everything again after the node has been
	// // cleared and merged?
	// assertIndexCanAddAndFindN(t, index, totalEntries)

	// // What about removing chunks of keys
	// t.Fatal("THIS IS BROKEN!")
	// assertIndexCanRemoveReverseRange(t, index, uint32(totalEntries)/2, uint32(totalEntries))
	// assertIndexCanRemoveReverseRange(t, index, 4001, uint32(totalEntries)/2-1)
	// assertIndexCanRemoveReverseRange(t, index, 511, 1020)
	// assertIndexCanRemoveRange(t, index, 1, 78)
	// // assertIndexCanFindRange(t, index, 78, 510)
	// assertIndexCanRemoveRange(t, index, 79, 510)
	// // assertIndexCanFindRange(t, index, 4000, 10000)
	// assertIndexCanRemoveRange(t, index, 1021, 4000)
	// assertIndexCanRemoveReverseRange(t, index, 4001, uint32(totalEntries)/2-1)
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
	repo := core.NewDataBlockRepository(dataBuffer)

	controlBlock := repo.ControlBlock()
	controlBlock.Format()
	dataBuffer.MarkAsDirty(controlBlock.DataBlockID())
	rootBlock, err := dataBuffer.FetchBlock(controlBlock.BTreeRootBlock())
	if err != nil {
		t.Fatal(err)
	}
	indexRoot := core.CreateBTreeLeaf(rootBlock)
	dataBuffer.MarkAsDirty(indexRoot.DataBlockID())

	blockMap := repo.DataBlocksMap()
	for i := uint16(0); i < 5; i++ {
		blockMap.MarkAsUsed(i)
	}

	return core.NewBTreeIndex(dataBuffer, repo, leafCapacity, branchCapacity)
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
		id := uint32(i + 1)
		expectedRowID := indexInsert(index, id, i)

		rowID, err := index.Find(id)
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

func assertIndexCanFindRange(t *testing.T, index core.BTreeIndex, firstID, lastID uint32) {
	for id := firstID; id <= lastID; id++ {
		expectedRowID := core.RowID{
			RecordID:    id,
			DataBlockID: uint16((id-1) % 10),
			LocalID:     uint16((id-1) % 100),
		}

		rowID, err := index.Find(id)
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
		indexInsert(index, uint32(i+1), i)
	}
}

func indexInsert(index core.BTreeIndex, id uint32, position int) core.RowID {
	rowID := core.RowID{
		RecordID:    id,
		DataBlockID: uint16(position % 10),
		LocalID:     uint16(position % 100),
	}
	index.Add(id, rowID)
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

func assertIndexCanRemoveReverseRange(t *testing.T, index core.BTreeIndex, firstID, lastID uint32) {
	totalBefore := len(index.All())
	for id := lastID; id >= firstID; id-- {
		index.Remove(id)
	}
	totalAfter := len(index.All())
	if totalBefore != totalAfter+int(lastID-firstID)+1 {
		t.Fatalf("Invalid data on index! before=%d, after=%d, firstid=%d, lastid=%d", totalBefore, totalAfter, firstID, lastID)
	}
}