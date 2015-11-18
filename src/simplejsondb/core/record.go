package core

type Record struct {
	ID   uint32
	Data []byte
}

type RowID struct {
	DataBlockID uint16
	LocalID     uint16
}
