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
	Dump() string
}

func NewUint32Index(buffer dbio.DataBuffer, branchCapacity, leafCapacity int) Uint32Index {
	repo := NewDataBlockRepository(buffer)
	adapter := &uint32IndexNodeAdapter{buffer, repo}
	tree := bplustree.New(bplustree.Config{
		Adapter:        adapter,
		LeafCapacity:   leafCapacity,
		BranchCapacity: branchCapacity,
	})
	return &index{tree, adapter}
}

type index struct {
	tree    bplustree.BPlusTree
	adapter *uint32IndexNodeAdapter
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
	return i.tree.Delete(Uint32Key(key))
}

func (i *index) Find(key uint32) (RowID, error) {
	item, err := i.tree.Find(Uint32Key(key))
	if err != nil {
		return RowID{}, err
	}

	return item.(RowID), err
}

func (i *index) Insert(key uint32, rowID RowID) error {
	return i.tree.Insert(Uint32Key(key), rowID)
}

func (i *index) Dump() string {
	return bplustree.DumpTree(i.tree, i.adapter)
}
