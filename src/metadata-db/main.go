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
	file, err := createDatafile("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer file.Close()

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

func createDatafile(filename string) (*os.File, error) {
	if _, err := os.Stat(filename); err == nil {
		log.Println("Datafile exists, reusing it")
		return os.OpenFile(filename, os.O_RDWR, 0666)
	}

	log.Println("Creating datafile...")
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
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
