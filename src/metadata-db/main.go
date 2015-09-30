package main

import (
	"log"
)

func main() {
	df, err := NewDatafile("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer df.Close()

	block, err := df.ReadBlock(0)
	if err != nil {
		panic(err)
	}
	log.Println(DatablockByteOrder.Uint16(block.Data[0:2]))
	log.Println(DatablockByteOrder.Uint16(block.Data[2:4]))

	DatablockByteOrder.PutUint16(block.Data[0:2], uint16(1))
	DatablockByteOrder.PutUint16(block.Data[2:4], uint16(99))
	DatablockByteOrder.PutUint16(block.Data[10:12], uint16(4))

	df.WriteBlock(block)
	log.Println("Done")
}
