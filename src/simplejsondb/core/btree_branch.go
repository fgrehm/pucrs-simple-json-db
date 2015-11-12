package core

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sort"
	"simplejsondb/dbio"
)

type BTreeBranch interface {
	BTreeNode
	Add(SearchKey uint32, leftNode, rightNode BTreeNode)
	// TODO: Append should be the only way to add nodes to a branch, the add above is just for creation
	Append(searchKey uint32, rightNode BTreeNode)
	Remove(searchKey uint32)
	Shift() BTreeBranchEntry
	Pop() BTreeBranchEntry
	ReplaceKey(oldValue, newValue uint32)
	Find(searchKey uint32) uint16
	All() []BTreeBranchEntry
	FirstEntry() BTreeBranchEntry
}

const (
	BTREE_BRANCH_MAX_ENTRIES           = 680
	BTREE_BRANCH_ENTRY_JUMP            = 6 // 2 bytes for the left pointer and 4 bytes for the search key
	BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID  = 0
	BTREE_BRANCH_OFFSET_KEY            = 2
	BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID = 6
)

type bTreeBranch struct {
	*bTreeNode
}

type BTreeBranchEntry struct {
	startsAt   uint16
	SearchKey  uint32
	GteBlockID uint16
	LtBlockID  uint16
}

func CreateBTreeBranch(block *dbio.DataBlock) BTreeBranch {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)
	node := &bTreeNode{block}
	return &bTreeBranch{node}
}

func (b *bTreeBranch) Find(searchKey uint32) uint16 {
	log.Infof("IDX_BRANCH_FIND blockid=%d, searchkey=%d", b.block.ID, searchKey)
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	if entriesCount == 0 {
		return 0
	}

	if lastEntry := b.lastEntry(); searchKey >= lastEntry.SearchKey {
		log.Infof("IDX_BRANCH_FIND_LAST entry=%+v", lastEntry)
		return lastEntry.GteBlockID
	}

	if firstEntry := b.FirstEntry(); searchKey < firstEntry.SearchKey {
		log.Infof("IDX_BRANCH_FIND_FIRST entry=%+v", firstEntry)
		return firstEntry.LtBlockID
	}

	// XXX: Should we perform a binary search here?
	entryToFollowKey := uint32(0)
	entryToFollowPtr := 0
	offset := int(BTREE_POS_ENTRIES_OFFSET) + BTREE_BRANCH_ENTRY_JUMP
	for i := 0; i < entriesCount; i++ {
		keyFound := b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)
		log.Infof("IDX_BRANCH_KEY_FOUND keyFound=%+v", keyFound)
		// We have a match!
		if keyFound >= searchKey {
			entryToFollowKey = keyFound
			entryToFollowPtr = offset
			break
		}
		offset += BTREE_BRANCH_ENTRY_JUMP
	}

	if entryToFollowPtr == 0 {
		panic("Something weird happened and an entry could not be found for a branch that is not empty")
	}

	if searchKey == entryToFollowKey {
		log.Infof("IDX_BRANCH_FIND_FOUND searchkey == entryKey=%+v", entryToFollowKey)
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID)
	} else {
		log.Infof("IDX_BRANCH_FIND_FOUND searchkey < entryKey=%+v", entryToFollowKey)
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID)
	}
}

func (b *bTreeBranch) Add(searchKey uint32, leftNode, rightNode BTreeNode) {
	entriesCount := b.EntriesCount()

	addPosition := sort.Search(int(entriesCount), func(i int) bool {
		offset := int(BTREE_POS_ENTRIES_OFFSET) + int(i)*int(BTREE_BRANCH_ENTRY_JUMP)
		keyFound := b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)
		return keyFound >= searchKey
	})
	writeOffset := int(BTREE_POS_ENTRIES_OFFSET) + int(addPosition)*int(BTREE_BRANCH_ENTRY_JUMP)
	if uint16(addPosition) < entriesCount && searchKey == b.block.ReadUint32(writeOffset+BTREE_BRANCH_OFFSET_KEY) {
		panic(fmt.Sprintf("Duplicate key detected: %d", searchKey))
	}

	b.block.Unshift(writeOffset+BTREE_BRANCH_OFFSET_KEY, BTREE_BRANCH_ENTRY_JUMP)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID, leftNode.DataBlockID())
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_KEY, searchKey)
	b.block.Write(writeOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, rightNode.DataBlockID())

	entriesCount += 1
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount))

	log.Infof("IDX_BRANCH_ADDED blockID=%d, searchKey=%d, position=%d, entriesCount=%d, writeOffset=%d, leftID=%d, rightID=%d", b.block.ID, searchKey, addPosition, entriesCount, writeOffset, leftNode.DataBlockID(), rightNode.DataBlockID())
}

func (b *bTreeBranch) Append(searchKey uint32, rightNode BTreeNode) {
	log.Infof("IDX_BRANCH_APPEND blockID=%d, searchKey=%d, rightID=%d", b.block.ID, searchKey, rightNode.DataBlockID())

	entriesCount := b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)

	// Since we always insert keys in order, we always append the values at the
	// end of the node
	initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (entriesCount * BTREE_BRANCH_ENTRY_JUMP))
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_KEY, searchKey)
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, rightNode.DataBlockID())

	entriesCount += 1
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount))
}

func (b *bTreeBranch) ReplaceKey(oldValue, newValue uint32) {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	log.Infof("IDX_BRANCH_REPLACE_KEY blockid=%d, old=%d, new=%d, entriescount=%d", b.block.ID, oldValue, newValue, entriesCount)

	// If there is only one entry on the node, just update the search key
	if entriesCount == 1 {
		log.Infof("IDX_BRANCH_REPLACE_KEY on first entry")
		b.block.Write(BTREE_POS_ENTRIES_OFFSET+BTREE_BRANCH_OFFSET_KEY, newValue)
		return
	}

	if lastEntry := b.lastEntry(); oldValue >= lastEntry.SearchKey {
		log.Infof("IDX_BRANCH_REPLACE_KEY on last entry %d", lastEntry.SearchKey)
		b.block.Write(int(lastEntry.startsAt+BTREE_BRANCH_OFFSET_KEY), newValue)
		return
	}

	if firstEntry := b.FirstEntry(); oldValue <= firstEntry.SearchKey {
		log.Infof("IDX_BRANCH_REPLACE_KEY on first entry %d", firstEntry.SearchKey)
		b.block.Write(int(firstEntry.startsAt+BTREE_BRANCH_OFFSET_KEY), newValue)
		return
	}

	// XXX: Should we perform a binary search here?
	offset := BTREE_POS_ENTRIES_OFFSET + BTREE_BRANCH_ENTRY_JUMP
	for i := 1; i < entriesCount-1; i++ {
		keyFound := b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)

		// We have a match!
		if keyFound >= oldValue {
			log.Infof("IDX_BRANCH_REPLACE_KEY on %dth entry %d", i, keyFound)
			b.block.Write(offset+BTREE_BRANCH_OFFSET_KEY, newValue)
			return
		}
		offset += BTREE_BRANCH_ENTRY_JUMP
	}
	panic("Something weird happened")
}

func (b *bTreeBranch) Remove(searchKey uint32) {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	log.Infof("IDX_BRANCH_REMOVE blockID=%d, searchKey=%d, entriesCount=%d", b.block.ID, searchKey, entriesCount)

	// If there is only one entry on the node, just update the counter
	if entriesCount == 1 {
		b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(0))
		return
	}

	// If we are removing the last key, just update the entries count and call it a day
	if lastEntry := b.lastEntry(); searchKey >= lastEntry.SearchKey {
		log.Infof("IDX_BRANCH_REMOVE_LAST keyFound=%d, searchKey=%d, ptr=%d", lastEntry.SearchKey, searchKey, lastEntry.startsAt)
		b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))
		return
	}

	// XXX: Should we perform a binary search here?
	entryToRemovePtr := 0
	entryToRemoveKey := uint32(0)
	offset := BTREE_POS_ENTRIES_OFFSET
	for i := 0; i < entriesCount-1; i++ {
		keyFound := b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)
		log.Infof("IDX_BRANCH_KEY_FOUND keyFound=%+v", keyFound)

		// We have a match!
		if keyFound >= searchKey {
			entryToRemovePtr = offset
			entryToRemoveKey = keyFound
			log.Infof("IDX_BRANCH_REMOVE keyFound=%d, searchKey=%d, position=%d, ptr=%d", keyFound, searchKey, i, entryToRemovePtr)
			break
		}
		offset += BTREE_BRANCH_ENTRY_JUMP
	}

	if entryToRemovePtr == 0 {
		panic(fmt.Sprintf("Unable to remove an entry with the key %d", searchKey))
	}

	// Write back the amount of entries on this block
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))

	if entryToRemoveKey == searchKey {
		// Keep the lower than pointer around
		entryToRemovePtr += BTREE_BRANCH_OFFSET_KEY
	} else {
		// Keep the lower than pointer around
		entryToRemovePtr -= BTREE_BRANCH_ENTRY_JUMP
		entryToRemovePtr += BTREE_BRANCH_OFFSET_KEY
	}

	log.Infof("IDX_BRANCH_REMOVE ptr=%d", entryToRemovePtr)

	// Copy data over
	lastByteToOverwrite := int(BTREE_POS_ENTRIES_OFFSET) + entriesCount*BTREE_BRANCH_ENTRY_JUMP
	for i := entryToRemovePtr; i < lastByteToOverwrite; i++ {
		b.block.Data[i] = b.block.Data[i+BTREE_BRANCH_ENTRY_JUMP]
	}
}

func (b *bTreeBranch) Pop() BTreeBranchEntry {
	if b.EntriesCount() == 0 {
		panic("Called Shift() on a leaf that has no entries")
	}
	lastEntry := b.lastEntry()
	b.Remove(lastEntry.SearchKey)
	return lastEntry
}

func (b *bTreeBranch) Shift() BTreeBranchEntry {
	if b.EntriesCount() == 0 {
		panic("Called Shift() on a leaf that has no entries")
	}
	firstEntry := b.FirstEntry()
	b.Remove(firstEntry.SearchKey)
	return firstEntry
}

func (b *bTreeBranch) FirstEntry() BTreeBranchEntry {
	offset := int(BTREE_POS_ENTRIES_OFFSET)
	return BTreeBranchEntry{
		startsAt:   uint16(offset),
		SearchKey:  b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY),
		LtBlockID:  b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID),
		GteBlockID: b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID),
	}
}

func (b *bTreeBranch) All() []BTreeBranchEntry {
	entriesCount := b.EntriesCount()
	entries := make([]BTreeBranchEntry, 0, entriesCount)

	offset := BTREE_POS_ENTRIES_OFFSET
	for i := uint16(0); i < entriesCount; i++ {
		entries = append(entries, BTreeBranchEntry{
			startsAt:   uint16(offset),
			SearchKey:  b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY),
			LtBlockID:  b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID),
			GteBlockID: b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID),
		})
		offset += BTREE_BRANCH_ENTRY_JUMP
	}

	return entries
}

func (b *bTreeBranch) lastEntry() BTreeBranchEntry {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))
	offset := int(BTREE_POS_ENTRIES_OFFSET) + (entriesCount-1)*BTREE_BRANCH_ENTRY_JUMP
	return BTreeBranchEntry{
		startsAt:   uint16(offset),
		SearchKey:  b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY),
		LtBlockID:  b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID),
		GteBlockID: b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID),
	}
}
