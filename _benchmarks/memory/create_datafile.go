package main

import (
	"encoding/binary"
	"os"
)

const (
	DATAFILE_SIZE  = 1024 * 1024 * 256 // 256 MB
	DATABLOCK_SIZE = 1024 * 4          // 4KB
	FRAMES_COUNT   = 512               // Positions
)

var (
	DatablockByteOrder = binary.BigEndian
)

func main() {
	file, err := os.Create("benchmark.dat")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Truncate(DATAFILE_SIZE)

	dataBlocksCount := DATAFILE_SIZE / DATABLOCK_SIZE

	// For each datablock, write the index so we can read it afterwards
	for i := 0; i < dataBlocksCount; i++ {
		if _, err := file.Seek(int64(i*DATABLOCK_SIZE), 0); err != nil {
			panic(err)
		}
		binary.Write(file, DatablockByteOrder, uint64(i))
	}
}
