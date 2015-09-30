package main

import (
	_ "bytes"
	_ "encoding/binary"
	_ "fmt"
	"os"
)

const (
	DATAFILE_SIZE = 1024 * 1024 * 256
)

func main() {
	file, err := os.Create("metadata-db.dat")
	if err != nil {
		panic(err)
	}

	file.Truncate(DATAFILE_SIZE)
}
