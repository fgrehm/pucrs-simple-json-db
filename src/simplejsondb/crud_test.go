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
	for i := 0; i < 31; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}

	fakeDataFile := utils.NewFakeDataFile(blocks)
	db, err := jsondb.NewWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	for i := 0; i < 4500; i++ {
		id := uint32(i + 1)
		data := fmt.Sprintf(`{"a":%d}`, i)
		err := db.InsertRecord(id, data)
		if err != nil {
			t.Fatalf("Unexpected error returned for the %d-th record: '%s'", id, err)
		}

		record, err := db.FindRecord(id)
		if err != nil {
			t.Fatalf("Unexpected error returned while reading %d (%s)", id, err)
		}
		if record.Data != data {
			t.Errorf("Unexpected data returned, got %s, expected %s", record.Data, data)
		}
	}
}

func TestCreateAndRemoveRecords(t *testing.T) {
	blocks := [][]byte{}
	for i := 0; i < 5; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}

	fakeDataFile := utils.NewFakeDataFile(blocks)
	db, err := jsondb.NewWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	for i := 0; i < 10; i++ {
		id := uint32(i + 1)
		data := fmt.Sprintf(`{"a":%d}`, i)
		err := db.InsertRecord(id, data)
		if err != nil {
			t.Fatalf("Unexpected error returned when inserting '%s'", err)
		}

		err = db.DeleteRecord(id)
		if err != nil {
			t.Fatalf("Unexpected error returned when removing '%s'", err)
		}

		if record, err := db.FindRecord(id); err == nil {
			t.Errorf("Expected error to be returned when finding %d, got nil and data '%s'", id, record.Data)
		}
	}
}

func TestCreateAndUpdateRecords(t *testing.T) {
	blocks := [][]byte{}
	for i := 0; i < 15; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}

	fakeDataFile := utils.NewFakeDataFile(blocks)
	db, err := jsondb.NewWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	// Insert some data
	for i := 0; i < 1000; i++ {
		data := fmt.Sprintf(`{"longest":%d}`, i)
		id := uint32(i + 1)
		err := db.InsertRecord(id, data)
		if err != nil {
			t.Fatalf("Unexpected error returned when inserting '%s'", err)
		}
	}

	// Shrink records
	for i := uint32(0); i < 1000; i++ {
		data := fmt.Sprintf(`{"a":%d}`, -int(i))
		id := i + 1

		err := db.UpdateRecord(id, data)
		if err != nil {
			t.Errorf("Unexpected error returned when updating record `%d` '%s'", id, err)
			continue
		}

		record, err := db.FindRecord(id)
		if err != nil {
			t.Errorf("Unexpected error returned while reading %d (%s)", id, err)
			continue
		}
		if record.Data != data {
			t.Errorf("Unexpected data returned, got `%s`, expected `%s`", record.Data, data)
			continue
		}
	}

	// Grow records again
	for i := uint32(0); i < 1000; i++ {
		data := fmt.Sprintf(`{"waaaaat":%d}`, int(i))
		id := i + 1

		err := db.UpdateRecord(id, data)
		if err != nil {
			t.Errorf("Unexpected error returned when updating record `%d` '%s'", id, err)
			continue
		}

		record, err := db.FindRecord(id)
		if err != nil {
			t.Errorf("Unexpected error returned while reading %d (%s)", id, err)
			continue
		}
		if record.Data != data {
			t.Errorf("Unexpected data returned, got `%s`, expected `%s`", record.Data, data)
			continue
		}
	}
}
