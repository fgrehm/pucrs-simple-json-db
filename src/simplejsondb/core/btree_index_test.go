package core

import (
	"simplejsondb/dbio"

	utils "test_utils"
	"testing"
)

func TestBTreeIndex_LeafRootNode(t *testing.T) {
	index := createTestBTreeIndex(t, 1, 4)

	// Start by ensuring that we can't find an entry that does not exist
	if _, err := index.Find(9); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Fill block up to its limit and ensure we can read nodes that are not
	// completely full
	indexInsertAndFindN(t, index, 510)

	// Find all of the records we have just inserted
	indexFindN(t, index, 510)

	// Ensure we error when the block is full but the entry does not exist
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted
	indexRemoveN(index, 510)

	// Ensure we can't load the records anymore
	indexFindUnknownN(t, index, 510)

	// Add one record and ensure it can be removed
	index.Add(1, RowID{RecordID: 1})
	index.Remove(1)

	// Just as a sanity check, can we add everything again after the node has been
	// cleared?
	indexInsertAndFindN(t, index, 510)
}

func TestBTreeIndex_LeafRootSplit(t *testing.T) {
	index := createTestBTreeIndex(t, 3, 5)

	// Fill block up to its limit plus one and ensure we can read RowIDs back from
	// the index
	indexInsertAndFindN(t, index, 511)

	// Find all of the records we have just inserted
	indexFindN(t, index, 511)

	// Ensure we error when the leaf root node have been split and an unknown
	// record has been asked
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}
}

func TestBTreeIndex_LeafMergeToRoot(t *testing.T) {
	index := createTestBTreeIndex(t, 3, 5)

	// Fill block up to its limit plus one
	indexInsertN(index, 511)

	// Remove all of the records we have just inserted (AKA merge)
	indexRemoveN(index, 511)

	// Ensure we can't load the records anymore
	indexFindUnknownN(t, index, 511)

	// Just as a sanity check, can we add everything again after the node has been
	// cleared and merged?
	indexInsertAndFindN(t, index, 511)
}

func createTestBTreeIndex(t *testing.T, totalUsableBlocks, bufferFrames int) BTreeIndex {
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
	repo := NewDataBlockRepository(dataBuffer)

	controlBlock := repo.ControlBlock()
	controlBlock.Format()
	dataBuffer.MarkAsDirty(controlBlock.DataBlockID())
	rootBlock, err := dataBuffer.FetchBlock(controlBlock.BTreeRootBlock())
	if err != nil {
		t.Fatal(err)
	}
	indexRoot := CreateBTreeLeaf(rootBlock)
	dataBuffer.MarkAsDirty(indexRoot.DataBlockID())

	blockMap := repo.DataBlocksMap()
	for i := uint16(0); i < 5; i++ {
		blockMap.MarkAsUsed(i)
	}

	return &bTreeIndex{dataBuffer, repo}
}

func indexFindN(t *testing.T, index BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		id := uint32(i + 1)
		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}
		expectedRowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}
}

func indexFindUnknownN(t *testing.T, index BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		id := uint32(i + 1)
		rowID, err := index.Find(id)
		if err == nil {
			t.Fatalf("Did not return an error when finding the record %d: %+v", id, rowID)
		}
	}
}

func indexInsertAndFindN(t *testing.T, index BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		id := uint32(i+1)
		expectedRowID := indexInsert(index, id, uint16(i))

		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}
}

func indexInsertN(index BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		indexInsert(index, uint32(i+1), uint16(i))
	}
}

func indexInsert(index BTreeIndex, id uint32, position uint16) RowID {
	rowID := RowID{
		RecordID: id,
		DataBlockID: position % 10,
		LocalID:     position % 100,
	}
	index.Add(id, rowID)
	return rowID
}

func indexRemoveN(index BTreeIndex, totalRecords int) {
	for i := 0; i < totalRecords; i++ {
		id := uint32(i + 1)
		index.Remove(id)
	}
}
