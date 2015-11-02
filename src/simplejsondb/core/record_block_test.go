package core

import (
	"simplejsondb/dbio"

	"testing"
)

func TestRecordBlock_BasicAddReadAndDeleteFlow(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	prevUtilization := rb.Utilization()

	localID := rb.Add(uint32(10), []byte("01234567890123456789"))

	// "Force reload" the wrapper
	rb = &recordBlock{block}

	// Did the utilization increase?
	if rb.Utilization()-prevUtilization != RECORD_HEADER_SIZE+20 {
		t.Errorf("Allocated an unexpected amount of data, got %d, expected %d, total %d", rb.Utilization()-prevUtilization, RECORD_HEADER_SIZE+20, rb.Utilization())
	}

	// Can we read the record again?
	data, err := rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}

	if data != "01234567890123456789" {
		t.Errorf("Unexpected data found `%s`", data)
	}

	if err := rb.Remove(localID); err != nil {
		t.Fatal(err)
	}

	// "Force reload" the wrapper
	rb = &recordBlock{block}
	if _, err := rb.ReadRecordData(localID); err == nil {
		t.Error("Did not return an error")
	}

	if rb.Utilization()-prevUtilization != RECORD_HEADER_SIZE {
		t.Errorf("Freed an unexpected amount of data, got %d, expected %d, total %d", rb.Utilization()-prevUtilization, RECORD_HEADER_SIZE, rb.Utilization())
	}
}

// REFACTOR: This needs love
func TestRecordBlock_Allocation(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	localIDForUpdate := rb.Add(uint32(10), []byte("0123456789"))
	localID := rb.Add(uint32(11), []byte("AAAAAAAAAA"))
	rb.Add(uint32(12), []byte("9999999999"))

	if err := rb.Remove(localID); err != nil {
		t.Fatal(err)
	}
	prevUtilization := rb.Utilization()
	newLocalID := rb.Add(uint32(13), []byte("00"))
	if localID != newLocalID {
		t.Error("Did not reuse the deleted record localid")
	}

	if err := rb.SoftRemove(localIDForUpdate); err != nil {
		t.Fatal(err)
	}
	rb.Add(uint32(10), []byte("*UPDATED*"))

	// "Force reload" the wrapper
	rb = &recordBlock{block}

	// Can we read the record again?
	data, err := rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "00" {
		t.Errorf("Unexpected data found `%s`", data)
	}
	// Ensure things are persisted as they should
	if string(block.Data[0:21]) != "999999999900*UPDATED*" {
		t.Errorf("Did not defrag the block. Block contents `%s`, expected `%s`", string(block.Data[0:21]), "99999999900*UPDATED*")
	}

	// Ensure this will change
	prevUtilization = rb.Utilization()

	// Add yet another set of bytes
	rb.Add(uint32(14), []byte("NNNNNNNNNN"))

	// "Force reload" the wrapper
	rb = &recordBlock{block}

	// Did we update the utilization accordingly?
	if rb.Utilization()-prevUtilization != RECORD_HEADER_SIZE+10 {
		t.Errorf("Allocated an unexpected amount of data (allocated %d bytes, expected %d, total %d)", rb.Utilization()-prevUtilization, RECORD_HEADER_SIZE+10, rb.Utilization())
	}

	// Can we still read the record that filled in the gap?
	data, err = rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "00" {
		t.Errorf("Unexpected data found `%s`, expected `%s`", data, "00")
	}

	// Last but not least, ensure things are persisted as they should
	if string(block.Data[0:31]) != "999999999900*UPDATED*NNNNNNNNNN" {
		t.Errorf("Invalid final state `%s`, expected `%s`", string(block.Data[0:31]), "999999999900*UPDATED*NNNNNNNNNN")
	}
}

func TestRecordBlock_UpdateFlow(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	rb.Add(uint32(10), []byte("0123456789"))
	localID := rb.Add(uint32(11), []byte("AAAAAAAAAA"))
	rb.Add(uint32(12), []byte("9999999999"))

	if err := rb.SoftRemove(localID); err != nil {
		t.Fatal(err)
	}
	newLocalID := rb.Add(uint32(13), []byte("00"))
	if localID == newLocalID {
		t.Error("Reused the soft deleted record localid")
	}

	// "Force reload" the wrapper
	rb = &recordBlock{block}

	// Should not be able to read the data at this point
	data, err := rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "" {
		t.Errorf("Read data for a record that has been deleted `%s`", data)
	}

	// Replace record that was soft deleted
	rb.Add(uint32(11), []byte("NNNNNNNNNN"))

	// And make sure it can be read again using the same localID
	data, err = rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "NNNNNNNNNN" {
		t.Errorf("Unexpected data found `%s`", data)
	}
}

func TestRecordBlock_ChainedRows(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	// Ensure we don't set chained rows for unkown records
	if err := rb.SetChainedRowID(1, RowID{}); err == nil {
		t.Fatal(err)
	}

	localID := rb.Add(uint32(14), []byte("NNNNNNNNNN"))
	chainedID := RowID{RecordID: 14, DataBlockID: 1, LocalID: 2}
	if err := rb.SetChainedRowID(localID, chainedID); err != nil {
		t.Fatal(err)
	}

	// "Force reload" the wrapper
	rb = &recordBlock{block}
	rowID, err := rb.ChainedRowID(localID)
	if err != nil {
		t.Fatal(err)
	}
	if rowID != chainedID {
		t.Fatalf("Invalid chained row ID found, %+v", rowID)
	}

	// Ensure the chained row id gets restored after removing and inserting a new record
	rb.Remove(localID)
	if newID := rb.Add(uint32(15), []byte("A")); newID != localID {
		t.Fatalf("Did not reuse the localID, got %d, expected %d", newID, localID)
	}
	rowID, err = rb.ChainedRowID(localID)
	if err != nil {
		t.Fatal(err)
	}
	if rowID != (RowID{RecordID: 15}) {
		t.Fatalf("Invalid chained row ID found, %+v", rowID)
	}
}

func TestRecordBlock_NextBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	if rb.NextBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetNextBlockID(99)

	// "Force reload" the wrapper
	rb = &recordBlock{block}
	if rb.NextBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}

func TestRecordBlock_PrevBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := &recordBlock{block}

	if rb.PrevBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetPrevBlockID(99)

	// "Force reload" the wrapper
	rb = &recordBlock{block}
	if rb.PrevBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}
