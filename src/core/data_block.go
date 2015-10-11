package core

import (
	"fmt"
)

type DataBlock struct {
	ID   uint16
	Data []byte
}

func (db *DataBlock) ReadUint16(startingAt int) uint16 {
	return DatablockByteOrder.Uint16(db.Data[startingAt : startingAt+2])
}

func (db *DataBlock) ReadUint32(startingAt int) uint32 {
	return DatablockByteOrder.Uint32(db.Data[startingAt : startingAt+4])
}

func (db *DataBlock) ReadString(startingAt, length int) string {
	return string(db.Data[startingAt : startingAt+length])
}

func (db *DataBlock) Write(position int, v interface{}) {
	switch x := v.(type) {
	case string:
		lastPosition := position + len(x)
		i := 0
		for target := position; target < lastPosition; target++ {
			db.Data[target] = byte(x[i])
			i++
		}
	case uint16:
		DatablockByteOrder.PutUint16(db.Data[position:position+2], x)
	case uint32:
		DatablockByteOrder.PutUint32(db.Data[position:position+4], x)
	default:
		panic(fmt.Sprintf("Don't know how to write %+v", x))
	}
}
