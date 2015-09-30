package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
)

const (
	DATAFILE_SIZE  = 1024 * 1024 * 256 // 256 MB
	DATABLOCK_SIZE = 1024 * 4          // 4KB
)

var (
	DatablockByteOrder = binary.BigEndian
)

func main() {
	file, err := createDatafile()
	if err != nil {
		panic(err)
	}

	writeInt16(file, 7)
	writeInt16(file, 1)

	if _, err := file.Seek(0, 0); err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		i, err := readInt16(file)
		if err != nil {
			panic(err)
		}

		println(i)
	}
}

func createDatafile() (*os.File, error) {
	file, err := os.Create("metadata-db.dat")
	if err != nil {
		return nil, err
	}

	log.Println("Creating datafile...")
	file.Truncate(DATAFILE_SIZE)
	log.Println("DONE")

	return file, nil
}

func writeInt16(f io.Writer, i uint16) error {
	log.Printf("Writing int16 `%d`", i)
	return binary.Write(f, DatablockByteOrder, i)
}

func readInt16(f io.Reader) (ret uint16, err error) {
	log.Println("Reading int16")
	err = binary.Read(f, DatablockByteOrder, &ret)
	return
}
