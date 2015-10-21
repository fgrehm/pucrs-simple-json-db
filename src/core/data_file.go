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

type DataFile interface {
	Close() error
	ReadBlock(id uint16, data []byte) error
	WriteBlock(id uint16, data []byte) error
}

type datafile struct {
	file *os.File
}

func newDatafile(filename string) (DataFile, error) {
	file, err := openDatafile(filename)
	if err != nil {
		return nil, err
	}

	return &datafile{file: file}, nil
}

func openDatafile(filename string) (*os.File, error) {
	if _, err := os.Stat(filename); err == nil {
		log.Println("DataFile exists, reusing it")
		return os.OpenFile(filename, os.O_RDWR, 0666)
	}

	log.Println("Creating datafile")
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	if err = file.Truncate(DATAFILE_SIZE); err != nil {
		return nil, err
	}
	return file, nil
}

func (df *datafile) ReadBlock(id uint16, data []byte) error {
	if _, err := df.file.Seek(int64(id*DATABLOCK_SIZE), 0); err != nil {
		return err
	}
	log.Printf("Reading datablock %010d", id)
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
	return df.file.Sync()
}

func (df *datafile) Close() error {
	log.Println("Closing datafile")
	return df.file.Close()
}
