package core

import (
  "simplejsondb/dbio"

  "testing"
)

func TestBTreeNode_Creation(t *testing.T) {
  block := &dbio.DataBlock{ID: 0, Data: make([]byte, 1)}
  var node BTreeNode = CreateBTreeLeaf(block)
  if !node.IsLeaf() {
    t.Error("Node was not recognized as a leaf node")
  }

  block = &dbio.DataBlock{ID: 0, Data: make([]byte, 1)}
  node = CreateBTreeBranch(block)
  if node.IsLeaf() {
    t.Error("Node was not recognized as a branch node")
  }
}
