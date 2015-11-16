package core

import (
  "bplustree"
  "simplejsondb/dbio"
)

type uint32IndexNodeAdapter struct {
  buffer dbio.DataBuffer
}

type uint32IndexLeafNode struct {
//   id       Uint16ID
//   parentID Uint16ID
//   leftID   Uint16ID
//   rightID  Uint16ID
//   entries  LeafEntries
}
type uint32IndexBranchNode struct {
//   id       Uint16ID
//   parentID Uint16ID
//   leftID   Uint16ID
//   rightID  Uint16ID
//   entries  BranchEntries
}

func (a *uint32IndexNodeAdapter) SetRoot(node bplustree.Node) {
  panic("NOT WORKING YET")
  // a.rootID = node.ID().(Uint16ID)
  // node.SetParentID(Uint16ID(0))
}

func (a *uint32IndexNodeAdapter) Init() bplustree.LeafNode {
  panic("NOT WORKING YET")
  return nil
  // root := a.CreateLeaf()
  // a.rootID = root.ID().(Uint16ID)
  // a.firstLeafID = a.rootID
  // return root
}

func (a *uint32IndexNodeAdapter) IsRoot(node bplustree.Node) bool {
  panic("NOT WORKING YET")
  return false
  // return a.rootID == node.ID()
}

func (a *uint32IndexNodeAdapter) LoadRoot() bplustree.Node {
  panic("NOT WORKING YET")
  return nil
  // return a.Nodes[a.rootID]
}

func (a *uint32IndexNodeAdapter) LoadNode(id bplustree.NodeID) bplustree.Node {
  panic("NOT WORKING YET")
  return nil
  // node := a.Nodes[id.(Uint16ID)]
  // if node != nil {
  //   return node
  // } else {
  //   return nil
  // }
}

func (a *uint32IndexNodeAdapter) Free(node bplustree.Node) {
  panic("NOT WORKING YET")
  // delete(a.Nodes, node.ID().(Uint16ID))
}

func (a *uint32IndexNodeAdapter) LoadFirstLeaf() bplustree.LeafNode {
  panic("NOT WORKING YET")
  return nil // a.Nodes[a.firstLeafID].(LeafNode)
}

func (a *uint32IndexNodeAdapter) LoadBranch(id bplustree.NodeID) bplustree.BranchNode {
  panic("NOT WORKING YET")
  return nil
  // node := a.LoadNode(id)
  // if node != nil {
  //   return node.(bplustree.BranchNode)
  // } else {
  //   return nil
  // }
}

func (a *uint32IndexNodeAdapter) LoadLeaf(id bplustree.NodeID) bplustree.LeafNode {
  panic("NOT WORKING YET")
  return nil
  // node := a.LoadNode(id)
  // if node != nil {
  //   return node.(LeafNode)
  // } else {
  //   return nil
  // }
}

func (a *uint32IndexNodeAdapter) CreateLeaf() bplustree.LeafNode {
  panic("NOT WORKING YET")
  return nil
  // node := &uint32IndexLeafNode{id: Uint16ID(a.nextNodeID)}
  // a.Nodes[node.id] = node
  // a.nextNodeID += 1
  // return node
}

func (a *uint32IndexNodeAdapter) CreateBranch(entry bplustree.BranchEntry) bplustree.BranchNode {
  panic("NOT WORKING YET")
  return nil
  // node := &uint32IndexBranchNode{id: Uint16ID(a.nextNodeID)}
  // node.entries = BranchEntries{entry}
  // a.Nodes[node.id] = node
  // a.nextNodeID += 1
  // return node
}

func (l *uint32IndexLeafNode) ID() bplustree.NodeID {
  panic("NOT WORKING YET")
  return nil
  // return bplustree.NodeID(l.id)
}

func (l *uint32IndexLeafNode) ParentID() bplustree.NodeID {
  panic("NOT WORKING YET")
  return nil
  // return bplustree.NodeID(l.parentID)
}

func (l *uint32IndexLeafNode) SetParentID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // l.parentID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) LeftSiblingID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(l.leftID)
}

func (l *uint32IndexLeafNode) SetLeftSiblingID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // l.leftID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) RightSiblingID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(l.rightID)
}

func (l *uint32IndexLeafNode) SetRightSiblingID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // l.rightID = id.(Uint16ID)
}

func (l *uint32IndexLeafNode) TotalKeys() int {
  panic("NOT WORKING YET")
  // return len(l.entries)
}

func (l *uint32IndexLeafNode) InsertAt(position int, entry bplustree.LeafEntry) {
  panic("NOT WORKING YET")
  // if position == len(l.entries) {
  //   l.entries = append(l.entries, entry)
  // } else if position == 0 {
  //   l.entries = append(LeafEntries{entry}, l.entries...)
  // } else {
  //   l.entries = append(l.entries, entry)
  //   copy(l.entries[position+1:], l.entries[position:])
  //   l.entries[position] = entry
  // }
}

func (l *uint32IndexLeafNode) KeyAt(position int) bplustree.Key {
  panic("NOT WORKING YET")
  return nil
  // return l.entries[position].Key
}

func (l *uint32IndexLeafNode) ItemAt(position int) bplustree.Item {
  panic("NOT WORKING YET")
  return nil
  // return l.entries[position].Item
}

func (l *uint32IndexLeafNode) DeleteAt(position int) bplustree.LeafEntry {
  panic("NOT WORKING YET")
  return bplustree.LeafEntry{}
  // deleted := l.entries[position]
  // l.entries = append(l.entries[:position], l.entries[position+1:]...)
  // return deleted
}

func (l *uint32IndexLeafNode) DeleteFrom(startPosition int) bplustree.LeafEntries {
  panic("NOT WORKING YET")
  // deleted := l.entries[startPosition:]
  // l.entries = l.entries[0:startPosition]
  // return deleted
}

func (l *uint32IndexLeafNode) All(iterator bplustree.LeafEntriesIterator) error {
  panic("NOT WORKING YET")
  // for _, entry := range l.entries {
  //   iterator(entry)
  // }
  // return nil
}

func (b *uint32IndexBranchNode) ID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(b.id)
}

func (b *uint32IndexBranchNode) ParentID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(b.parentID)
}

func (b *uint32IndexBranchNode) SetParentID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // b.parentID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) LeftSiblingID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(b.leftID)
}

func (b *uint32IndexBranchNode) SetLeftSiblingID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // b.leftID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) RightSiblingID() bplustree.NodeID {
  panic("NOT WORKING YET")
  // return bplustree.NodeID(b.rightID)
}

func (b *uint32IndexBranchNode) SetRightSiblingID(id bplustree.NodeID) {
  panic("NOT WORKING YET")
  // b.rightID = id.(Uint16ID)
}

func (b *uint32IndexBranchNode) KeyAt(position int) bplustree.Key {
  panic("NOT WORKING YET")
  // return b.entries[position].Key
}

func (b *uint32IndexBranchNode) EntryAt(position int) bplustree.BranchEntry {
  panic("NOT WORKING YET")
  // return b.entries[position]
}

func (b *uint32IndexBranchNode) Append(key bplustree.Key, gteNodeID bplustree.NodeID) {
  panic("NOT WORKING YET")
  // entry := bplustree.BranchEntry{
  //   bplustree.Key:                           key,
  //   LowerThanKeyNodeID:            b.entries[len(b.entries)-1].LowerThanKeyNodeID,
  //   GreaterThanOrEqualToKeyNodeID: gteNodeID,
  // }
  // b.entries = append(b.entries, entry)
}

func (l *uint32IndexBranchNode) DeleteAt(position int) bplustree.BranchEntry {
  panic("NOT WORKING YET")
  // entry := l.entries[position]
  // if position == len(l.entries)-1 {
  //   l.entries = l.entries[0:position]
  // } else if position == 0 {
  //   l.entries = l.entries[1:]
  // } else {
  //   l.entries[position+1].LowerThanKeyNodeID = l.entries[position-1].GreaterThanOrEqualToKeyNodeID
  //   l.entries = append(l.entries[:position], l.entries[position+1:]...)
  // }
  // return entry
}

func (b *uint32IndexBranchNode) ReplaceKeyAt(position int, key bplustree.Key) {
  panic("NOT WORKING YET")
  // b.entries[position].Key = key
}

func (b *uint32IndexBranchNode) DeleteFrom(startPosition int) bplustree.BranchEntries {
  panic("NOT WORKING YET")
  // removed := b.entries[startPosition:]
  // b.entries = b.entries[0:startPosition]
  // return removed
}

func (b *uint32IndexBranchNode) Shift() {
  panic("NOT WORKING YET")
  // ltNodeID := b.entries[0].LowerThanKeyNodeID
  // b.entries = b.entries[1:]
  // b.entries[0].LowerThanKeyNodeID = ltNodeID
}

func (b *uint32IndexBranchNode) TotalKeys() int {
  panic("NOT WORKING YET")
  // return len(b.entries)
}

func (b *uint32IndexBranchNode) All(iterator bplustree.BranchEntriesIterator) error {
  panic("NOT WORKING YET")
  // for _, entry := range b.entries {
  //   iterator(entry)
  // }
  return nil
}

func (b *uint32IndexBranchNode) InsertAt(position int, key bplustree.Key, greaterThanOrEqualToKeyNodeID bplustree.NodeID) {
  panic("NOT WORKING YET")
  // entry := bplustree.BranchEntry{
  //   bplustree.Key: key,
  //   GreaterThanOrEqualToKeyNodeID: greaterThanOrEqualToKeyNodeID,
  // }

  // if position < 0 {
  //   panic("IS THIS CORRECT?")
  // } else if position == 0 {
  //   entry.LowerThanKeyNodeID = b.entries[0].LowerThanKeyNodeID
  //   b.entries[0].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
  //   b.entries = append(BranchEntries{entry}, b.entries...)
  // } else if position == len(b.entries) {
  //   entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
  //   b.entries = append(b.entries, entry)
  // } else {
  //   entry.LowerThanKeyNodeID = b.entries[position-1].GreaterThanOrEqualToKeyNodeID
  //   b.entries[position].LowerThanKeyNodeID = greaterThanOrEqualToKeyNodeID
  //   b.entries = append(b.entries, entry)
  //   copy(b.entries[position+1:], b.entries[position:])
  //   b.entries[position] = entry
  // }
}

func (b *uint32IndexBranchNode) Unshift(key bplustree.Key, lowerThanKeyNodeID bplustree.NodeID) {
  panic("NOT WORKING YET")
  // entry := bplustree.BranchEntry{
  //   bplustree.Key:                           key,
  //   LowerThanKeyNodeID:            lowerThanKeyNodeID,
  //   GreaterThanOrEqualToKeyNodeID: b.entries[0].LowerThanKeyNodeID,
  // }
  // b.entries = append(BranchEntries{entry}, b.entries...)
}
