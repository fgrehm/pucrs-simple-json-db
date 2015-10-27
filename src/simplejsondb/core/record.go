package core

type Record struct {
	ID   uint32
	Data string
}

type RowID struct {
	RecordID    uint32
	DataBlockID uint16
	LocalID     uint16
}
