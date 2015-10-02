package core

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

type Datafile interface {
	Close()
	ReadBlock(id uint16, data []byte) error
	WriteBlock(id uint16, data []byte) error
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

func (df *datafile) ReadBlock(id uint16, data []byte) error {
	if _, err := df.file.Seek(int64(id*DATABLOCK_SIZE), 0); err != nil {
		return err
	}
	log.Printf("Reading datablock %016d", id)
	reader := &io.LimitedReader{df.file, DATABLOCK_SIZE}
	_, err := reader.Read(data)
	return err
}

func (df *datafile) WriteBlock(id uint16, data []byte) error {
	if _, err := df.file.Seek(int64(id*DATABLOCK_SIZE), 0); err != nil {
		return err
	}
	log.Printf("Writing datablock %016d", id)
	if _, err := df.file.Write(data); err != nil {
		return err
	}
	df.file.Sync()

	return nil
}

func (df *datafile) Close() {
	df.file.Close()
}

// func (df *datafile) WriteInt16(position int64, i uint16) error {
// 	if _, err := df.file.Seek(position, 0); err != nil {
// 		return err
// 	}
// 	log.Printf("Writing int16 `%d`", i)
// 	return binary.Write(df.file, DatablockByteOrder, i)
// }
