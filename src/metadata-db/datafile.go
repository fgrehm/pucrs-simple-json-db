package main

import (
	"encoding/binary"
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

type Datafile interface {
	Close()
	WriteInt16(position int64, i uint16) error
	ReadInt16(position int64) (ret uint16, err error)
}

type datafile struct {
	file *os.File
}

func NewDatafile(filename string) (Datafile, error) {
	file, err := openDatafile(filename)
	if err != nil {
		return nil, err
	}

	return &datafile{file: file}, nil
}

func openDatafile(filename string) (*os.File, error) {
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

func (df *datafile) WriteInt16(position int64, i uint16) error {
	if _, err := df.file.Seek(position, 0); err != nil {
		return err
	}
	log.Printf("Writing int16 `%d`", i)
	return binary.Write(df.file, DatablockByteOrder, i)
}

func (df *datafile) ReadInt16(position int64) (ret uint16, err error) {
	if _, err := df.file.Seek(position, 0); err != nil {
		return 0, err
	}
	log.Println("Reading int16")
	err = binary.Read(df.file, DatablockByteOrder, &ret)
	return
}

func (df *datafile) Close() {
	df.file.Close()
}
