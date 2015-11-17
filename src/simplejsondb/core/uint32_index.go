package core

import (
	"bplustree"
	"simplejsondb/dbio"
)

type RowIDsIterator func(RowID)

type Uint32Key uint32

func (a Uint32Key) Less(b bplustree.Key) bool {
	return a < b.(Uint32Key)
}

type Uint16ID uint16

func (k Uint16ID) Equals(other bplustree.NodeID) bool {
	return k == other.(Uint16ID)
}

type Uint32Index interface {
	Insert(key uint32, item RowID) error
	Find(key uint32) (RowID, error)
	All(iterator RowIDsIterator) error
	Delete(key uint32) error
	Init()
}

func NewUint32Index(buffer dbio.DataBuffer, branchCapacity, leafCapacity int) Uint32Index {
	repo := NewDataBlockRepository(buffer)
	tree := bplustree.New(bplustree.Config{
		Adapter:        &uint32IndexNodeAdapter{buffer, repo},
		LeafCapacity:   leafCapacity,
		BranchCapacity: branchCapacity,
	})
	return &index{tree}
}

type index struct {
	tree bplustree.BPlusTree
}

func (i *index) Init() {
	i.tree.Init()
}

func (i *index) All(iterator RowIDsIterator) error {
	return i.tree.All(func(entry bplustree.LeafEntry) {
		iterator(entry.Item.(RowID))
	})
}

func (i *index) Delete(key uint32) error {
	panic("NOT WORKING YET")
}

func (i *index) Find(key uint32) (RowID, error) {
	panic("NOT WORKING YET")
}

func (i *index) Insert(key uint32, rowID RowID) error {
	return i.tree.Insert(Uint32Key(key), rowID)
}
