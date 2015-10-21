package core_test

import (
	"core"

	"fmt"
	"testing"
)

func TestCreateAndRetrieve(t *testing.T) {
	fakeDataFile := newFakeDataFile([][]byte{
		make([]byte, core.DATABLOCK_SIZE),
		make([]byte, core.DATABLOCK_SIZE),
	})
	db, err := core.NewMetaDBWithDataFile(fakeDataFile)
	if err != nil {
		t.Fatalf("Unexpected error returned '%s'", err)
	}

	for i := 0; i < 10; i++ {
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
