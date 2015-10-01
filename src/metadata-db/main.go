package main

import (
	"log"

  "core"
)

func main() {
	df, err := core.NewDatafile("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer df.Close()

	block, err := df.ReadBlock(0)
	if err != nil {
		panic(err)
	}
	log.Println(core.DatablockByteOrder.Uint16(block.Data[0:2]))
	log.Println(core.DatablockByteOrder.Uint16(block.Data[2:4]))

	core.DatablockByteOrder.PutUint16(block.Data[0:2], uint16(1))
	core.DatablockByteOrder.PutUint64(block.Data[2:10], uint64(9999))
	core.DatablockByteOrder.PutUint16(block.Data[14:16], uint16(4))

	df.WriteBlock(block)
	log.Println("Done")
}
