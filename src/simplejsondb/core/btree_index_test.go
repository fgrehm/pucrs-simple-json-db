package core

import (

	log "github.com/Sirupsen/logrus"
	"simplejsondb/dbio"

	utils "test_utils"
	"testing"
)

func TestBTreeIndex_LeafRootNode(t *testing.T) {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 4)
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

	index := bTreeIndex{dataBuffer, repo}

	// Start by ensuring that we can't find an entry that does not exist
	if _, err := index.Find(9); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Fill block up to its limit and ensure we can read nodes that are not
	// completely full
	for i := 0; i < 510; i++ {
		id := uint32(i + 1)
		expectedRowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}
		index.Add(id, expectedRowID)

		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}

	// Find all of the records we have just inserted
	for i := 0; i < 510; i++ {
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

	// Ensure we error when the block is full but the entry does not exist
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}

	// Remove all of the records we have just inserted
	for i := 0; i < 510; i++ {
		id := uint32(i + 1)
		index.Remove(id)
	}

	// Ensure we can't load the records anymore
	for i := 0; i < 510; i++ {
		id := uint32(i + 1)
		rowID, err := index.Find(id)
		if err == nil {
			t.Fatalf("Did not return an error when finding the record %d: %+v", id, rowID)
		}
	}

	// Just as a sanity check, can we add everything again after the node has been
	// cleared?
	for i := 0; i < 510; i++ {
		id := uint32(i + 1)
		expectedRowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}
		index.Add(id, expectedRowID)

		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}
}

func TestBTreeIndex_LeafSplit(t *testing.T) {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 5)
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

	index := bTreeIndex{dataBuffer, repo}

	// Fill block up to its limit plus one and ensure we can read RowIDs back from
	// the index
	for i := 0; i < 511; i++ {
		id := uint32(i + 1)
		expectedRowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}
		index.Add(id, expectedRowID)

		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}

	// Find all of the records we have just inserted
	for i := 0; i < 511; i++ {
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

	// Ensure we error when the leaf root node have been split and an unknown
	// record has been asked
	if _, err := index.Find(9999); err == nil {
		t.Fatal("Did not return an error when finding a record that does not exist")
	}
}

func TestBTreeIndex_LeafMerge(t *testing.T) {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 5)
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

	index := bTreeIndex{dataBuffer, repo}

	// Fill block up to its limit plus one and ensure we can read RowIDs back from
	// the index
	for i := 0; i < 511; i++ {
		id := uint32(i + 1)
		rowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}
		index.Add(id, rowID)
	}

	// Remove all of the records we have just inserted
	for i := 0; i < 511; i++ {
		id := uint32(i + 1)
		index.Remove(id)
	}

	// Ensure we can't load the records anymore
	for i := 0; i < 511; i++ {
		id := uint32(i + 1)
		rowID, err := index.Find(id)
		if err == nil {
			t.Fatalf("Did not return an error when finding the record %d: %+v", id, rowID)
		}
	}

	// Just as a sanity check, can we add everything again after the node has been
	// cleared and merged?
	log.SetLevel(log.DebugLevel)
	for i := 0; i < 511; i++ {
		id := uint32(i + 1)
		expectedRowID := RowID{RecordID: id, DataBlockID: uint16(i % 10), LocalID: uint16(i % 100)}
		index.Add(id, expectedRowID)

		rowID, err := index.Find(id)
		if err != nil {
			t.Fatalf("Error while fetching %d: %s", id, err)
		}

		if rowID != expectedRowID {
			t.Fatalf("Wrong RowID found for record %d, got %+v, expected %+v", id, rowID, expectedRowID)
		}
	}
}
