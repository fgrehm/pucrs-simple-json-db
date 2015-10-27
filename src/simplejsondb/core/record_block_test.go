package core_test

import (
	"simplejsondb/core"
	"simplejsondb/dbio"

	"testing"
)

func TestRecordBlock_BasicFlow(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	prevUtilization := rb.Utilization()

	_, localID := rb.Add(uint32(10), []byte("01234567890123456789"))

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)

	// Did the utilization increase?
	if rb.Utilization()-prevUtilization != core.RECORD_HEADER_SIZE+20 {
		t.Errorf("Allocated an unexpected amount of data")
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
	rb = core.NewRecordBlock(block)
	if _, err := rb.ReadRecordData(localID); err == nil {
		t.Errorf("Did not return an error")
	}
}

func TestRecordBlock_NextBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	if rb.NextBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetNextBlockID(99)

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)
	if rb.NextBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}

func TestRecordBlock_PrevBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	if rb.PrevBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetPrevBlockID(99)

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)
	if rb.PrevBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}

// func TestRecordBlock_MakesUseOfEmptySlots(t *testing.T) {
// 	t.Fatal("TODO")
//
// 	// No chained row
// }
//
// func TestRecordBlock_ReturnsTheAmountOfBytesWritten(t *testing.T) {
// 	t.Fatal("TODO")
//
// 	// Force a chained row
// 	// test that a record that fits return all bytes
// }
