package core

import (
  "bplustree"
  "simplejsondb/dbio"
)

type Uint16NodeID uint16
type RowIDsIterator func(RowID)

type Uint32Key uint32
func (a Uint32Key) Less(b bplustree.Key) bool {
  return a < b.(Uint32Key)
}

type Uint32Index interface {
  Insert(key uint32, item RowID) error
  Find(key uint32) (RowID, error)
  All(iterator RowIDsIterator) error
  Delete(key uint32) error
}

func NewUint32Index(buffer dbio.DataBuffer, branchCapacity, leafCapacity int) Uint32Index {
  tree := bplustree.New(bplustree.Config{
    Adapter:        &uint32IndexNodeAdapter{buffer},
    LeafCapacity:   leafCapacity,
    BranchCapacity: branchCapacity,
  })
  return &index{tree}
}

type index struct {
  tree bplustree.BPlusTree
}

func (i *index) All(iterator RowIDsIterator) error {
  panic("NOT WORKING YET")
}

func (i *index) Delete(key uint32) error {
  panic("NOT WORKING YET")
}

func (i *index) Find(key uint32) (RowID, error) {
  panic("NOT WORKING YET")
}

func (i *index) Insert(key uint32, rowID RowID) error {
  panic("NOT WORKING YET")
}
