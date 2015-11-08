package core

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"simplejsondb/dbio"
)

type BTreeBranch interface {
	BTreeNode
	Add(searchKey uint32, leftNode, rightNode BTreeNode)
	Remove(searchKey uint32)
	ReplaceKey(oldValue, newValue uint32)
	Find(searchKey uint32) uint16
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

type bTreeBranchEntry struct {
	startsAt   uint16
	searchKey  uint32
	gteBlockID uint16
	ltBlockID  uint16
}

func CreateBTreeBranch(block *dbio.DataBlock) BTreeBranch {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)
	node := &bTreeNode{block}
	return &bTreeBranch{node}
}

func (b *bTreeBranch) Find(searchKey uint32) uint16 {
	log.Infof("BRANCH_FIND blockid=%d, searchkey=%d", b.block.ID, searchKey)
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	if entriesCount == 0 {
		return 0
	}

	if lastEntry := b.lastEntry(); searchKey >= lastEntry.searchKey {
		log.Infof("BRANCH_FIND_LAST entry=%+v", lastEntry)
		return lastEntry.gteBlockID
	}

	if firstEntry := b.firstEntry(); searchKey < firstEntry.searchKey {
		log.Infof("BRANCH_FIND_FIRST entry=%+v", firstEntry)
		return firstEntry.ltBlockID
	}

	// XXX: Should we perform a binary search here?
	entryToFollowKey := uint32(0)
	entryToFollowPtr := 0
	offset := int(BTREE_POS_ENTRIES_OFFSET) + BTREE_BRANCH_ENTRY_JUMP
	for i := 0; i < entriesCount; i++ {
		keyFound := b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY)
		log.Infof("BRANCH_KEY_FOUND keyFound=%+v", keyFound)
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

	if searchKey >= entryToFollowKey {
		log.Infof("BRANCH_FIND_FOUND searchkey >= entryKey=%+v", entryToFollowKey)
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID)
	} else {
		log.Infof("BRANCH_FIND_FOUND searchkey < entryKey=%+v", entryToFollowKey)
		return b.block.ReadUint16(entryToFollowPtr + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID)
	}
}

func (b *bTreeBranch) Add(searchKey uint32, leftNode, rightNode BTreeNode) {
	log.Infof("IDX_BRANCH_ADD blockid=%d, searchkey=%d, leftid=%d, rightid=%d", b.block.ID, searchKey, leftNode.DataBlockID(), rightNode.DataBlockID())

	entriesCount := b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)

	// Since we always insert keys in order, we always append the values at the
	// end of the node
	initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (entriesCount * BTREE_BRANCH_ENTRY_JUMP))
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID, leftNode.DataBlockID())
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_KEY, searchKey)
	b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID, rightNode.DataBlockID())

	entriesCount += 1
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount))
}

func (b *bTreeBranch) ReplaceKey(oldValue, newValue uint32) {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	log.Debugf("REPLACE_KEY blockid=%d, old=%d, new=%d, entriescount=%d", b.block.ID, oldValue, newValue, entriesCount)

	// If there is only one entry on the node, just update the search key
	if entriesCount == 1 {
		log.Debugf("REPLACE_KEY on first entry")
		b.block.Write(BTREE_POS_ENTRIES_OFFSET+BTREE_BRANCH_OFFSET_KEY, newValue)
		return
	}

	if lastEntry := b.lastEntry(); oldValue >= lastEntry.searchKey {
		log.Debugf("REPLACE_KEY on last entry %d", lastEntry.searchKey)
		b.block.Write(int(lastEntry.startsAt+BTREE_BRANCH_OFFSET_KEY), newValue)
		return
	}

	if firstEntry := b.firstEntry(); oldValue <= firstEntry.searchKey {
		log.Debugf("REPLACE_KEY on first entry %d", firstEntry.searchKey)
		b.block.Write(int(firstEntry.startsAt+BTREE_BRANCH_OFFSET_KEY), newValue)
		return
	}

	// XXX: Should we perform a binary search here?
	for i := 1; i < entriesCount-1; i++ {
		initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_BRANCH_ENTRY_JUMP))
		keyFound := b.block.ReadUint32(initialOffset + BTREE_BRANCH_OFFSET_KEY)

		// We have a match!
		if keyFound >= oldValue {
			log.Debugf("REPLACE_KEY on %dth entry %d", i, keyFound)
			b.block.Write(initialOffset+BTREE_BRANCH_OFFSET_KEY, newValue)
			return
		}
	}
	panic("Something weird happened")
}

func (b *bTreeBranch) Remove(searchKey uint32) {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))

	log.Infof("BRANCH_REMOVE blockid=%d, searchkey=%d, entriescount=%d", b.block.ID, searchKey, entriesCount)

	// If there is only one entry on the node, just update the counter
	if entriesCount == 1 {
		b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(0))
		return
	}

	// If we are removing the last key, just update the entries count and call it a day
	if lastEntry := b.lastEntry(); searchKey >= lastEntry.searchKey {
		log.Infof("BRANCH_REMOVE_LAST keyfound=%d, searchkey=%d, ptr=%d", lastEntry.searchKey, lastEntry.searchKey, lastEntry.startsAt)
		b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))
		return
	}

	// XXX: Should we perform a binary search here?
	entryToRemovePtr := 0
	for i := 0; i < entriesCount; i++ {
		initialOffset := int(BTREE_POS_ENTRIES_OFFSET + (i * BTREE_BRANCH_ENTRY_JUMP))
		keyFound := b.block.ReadUint32(initialOffset + BTREE_BRANCH_OFFSET_KEY)

		// We have a match!
		if searchKey >= keyFound {
			entryToRemovePtr = initialOffset
			log.Infof("BRANCH_REMOVE keyfound=%d, searchkey=%d, position=%d, ptr=%d", keyFound, searchKey, i, entryToRemovePtr)
			break
		}
	}

	if entryToRemovePtr == 0 {
		panic(fmt.Sprintf("Unable to remove an entry with the key %d", searchKey))
	}

	if entryToRemovePtr == -1 {
		panic(fmt.Sprintf("Tried to remove an entry that does not exist on the index: %d", searchKey))
	}

	// Write back the amount of entries on this block
	b.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(entriesCount-1))

	// Keep the lower than pointer around
	entryToRemovePtr += BTREE_BRANCH_OFFSET_KEY

	// Copy data over
	lastByteToOverwrite := int(BTREE_POS_ENTRIES_OFFSET) + (entriesCount-1)*BTREE_BRANCH_ENTRY_JUMP
	for i := entryToRemovePtr; i < lastByteToOverwrite; i++ {
		b.block.Data[i] = b.block.Data[i+BTREE_BRANCH_ENTRY_JUMP]
	}
}

func (b *bTreeBranch) firstEntry() bTreeBranchEntry {
	offset := int(BTREE_POS_ENTRIES_OFFSET)
	return bTreeBranchEntry{
		startsAt:   uint16(offset),
		searchKey:  b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY),
		ltBlockID:  b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID),
		gteBlockID: b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID),
	}
}

func (b *bTreeBranch) lastEntry() bTreeBranchEntry {
	entriesCount := int(b.block.ReadUint16(BTREE_POS_ENTRIES_COUNT))
	offset := int(BTREE_POS_ENTRIES_OFFSET) + (entriesCount-1)*BTREE_BRANCH_ENTRY_JUMP
	return bTreeBranchEntry{
		startsAt:   uint16(offset),
		searchKey:  b.block.ReadUint32(offset + BTREE_BRANCH_OFFSET_KEY),
		ltBlockID:  b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_LEFT_BLOCK_ID),
		gteBlockID: b.block.ReadUint16(offset + BTREE_BRANCH_OFFSET_RIGHT_BLOCK_ID),
	}
}
