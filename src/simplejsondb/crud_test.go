package simplejsondb_test

import (
	"fmt"
	"testing"

	jsondb "simplejsondb"
	dbio "simplejsondb/dbio"

	utils "test_utils"
)

func TestCreateAndRetrieveLotsOfRecords(t *testing.T) {
	blocks := [][]byte{}
	for i := 0; i < 26; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}

	fakeDataFile := utils.NewFakeDataFile(blocks)
	db, err := jsondb.NewWithDataFile(fakeDataFile)
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
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}

	fakeDataFile := utils.NewFakeDataFile(blocks)
	db, err := jsondb.NewWithDataFile(fakeDataFile)
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
