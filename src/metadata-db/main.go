package main

import (
	"log"
	"os"
)

const (
	DATAFILE_SIZE  = 1024 * 1024 * 256 // 256 MB
	DATABLOCK_SIZE = 1024 * 4          // 4KB
)

func main() {
	file, err := os.Create("metadata-db.dat")
	if err != nil {
		panic(err)
	}

	log.Println("Creating datafile...")
	file.Truncate(DATAFILE_SIZE)
	log.Println("DONE")
}
