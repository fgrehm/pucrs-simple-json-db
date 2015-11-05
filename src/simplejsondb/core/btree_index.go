package core

import (
	"fmt"

	"simplejsondb/dbio"
)

type BTreeIndex interface {
	Add(searchKey uint32, rowID RowID)
	Find(searchKey uint32) (RowID, error)
	Remove(searchKey uint32)
	All() []RowID
}

type bTreeIndex struct {
	buffer dbio.DataBuffer
	repo DataBlockRepository
}

func (idx *bTreeIndex) Add(searchKey uint32, rowID RowID) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeLeaf(controlBlock.FirstBTreeDataBlock())
	if !root.IsLeaf() {
		panic("Inserting on a root node made of a branch node is not supported yet")
	}

	root.Add(searchKey, rowID)
	idx.buffer.MarkAsDirty(root.DataBlockID())
}

func (idx *bTreeIndex) Find(searchKey uint32) (RowID, error) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeLeaf(controlBlock.FirstBTreeDataBlock())
	if !root.IsLeaf() {
		panic("Finding from a root node made of a branch node is not supported yet")
	}

	rowID := root.Find(searchKey)
	if rowID == (RowID{}) {
		return rowID, fmt.Errorf("Search key not found: %d", searchKey)
	}
	return rowID, nil
}

func (idx *bTreeIndex) Remove(searchKey uint32) {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeLeaf(controlBlock.FirstBTreeDataBlock())
	if !root.IsLeaf() {
		panic("Removing from a root node made of a branch node is not supported yet")
	}

	root.Remove(searchKey)
	idx.buffer.MarkAsDirty(root.DataBlockID())
}

func (idx *bTreeIndex) All() []RowID {
	controlBlock := idx.repo.ControlBlock()
	root := idx.repo.BTreeLeaf(controlBlock.FirstBTreeDataBlock())
	if !root.IsLeaf() {
		panic("Removing from a root node made of a branch node is not supported yet")
	}

	return root.All()
}
