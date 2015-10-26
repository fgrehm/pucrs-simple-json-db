package core_test

import (
	"core"

	"fmt"
	"testing"
)

func TestCreateAndRetrieveLotsOfRecords(t *testing.T) {
	blocks := [][]byte{}
	for i := 0; i < 26; i++ {
		blocks = append(blocks, make([]byte, core.DATABLOCK_SIZE))
	}

	fakeDataFile := newFakeDataFile(blocks)
	db, err := core.NewMetaDBWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	for i := 0; i < 4500; i++ {
		data := fmt.Sprintf(`{"a":%d}`, i)
		id, err := db.InsertRecord(data)
		if err != nil {
			t.Fatalf("Unexpected error returned '%s'", err)
		}

		record, err := db.FindRecord(id)
		if err != nil {
			t.Fatalf("Unexpected error returned '%s'", err)
		}
		if record.Data != data {
			t.Errorf("Unexpected data returned (%s)", record.Data)
		}
	}
}

func TestCreateAndRemoveRecords(t *testing.T) {
	blocks := [][]byte{}
	for i := 0; i < 4; i++ {
		blocks = append(blocks, make([]byte, core.DATABLOCK_SIZE))
	}

	fakeDataFile := newFakeDataFile(blocks)
	db, err := core.NewMetaDBWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	for i := 0; i < 10; i++ {
		data := fmt.Sprintf(`{"a":%d}`, i)
		id, err := db.InsertRecord(data)
		if err != nil {
			t.Fatalf("Unexpected error returned when inserting '%s'", err)
		}

		err = db.RemoveRecord(id)
		if err != nil {
			t.Fatalf("Unexpected error returned when removing '%s'", err)
		}

		if record, err := db.FindRecord(id); err == nil {
			t.Errorf("Expected error to be returned when finding %d, got nil and data '%s'", id, record.Data)
		}
	}
}
