package bplustree_test

import (
	"fmt"
	"testing"

	. "bplustree"
)

var adapter *InMemoryAdapter

func TestBPlusTree_InsertAndRetrieveOnLeaf(t *testing.T) {
	tree := createTree(8, 6)

	items := []Item{}
	for i := 1; i < 5; i++ {
		_, item := assertTreeCanInsertAndFind(t, tree, i, fmt.Sprintf("item-%d", i))
		items = append(items, item)
	}

	_, item := assertTreeCanInsertAndFind(t, tree, 0, "FIRST")
	items = append([]Item{item}, items...)
	_, item = assertTreeCanInsertAndFind(t, tree, 5, "LAST")
	items = append(items, item)

	assertTreeItemsAreSame(t, tree, items)
}

func TestBPlusTree_LeafRootSplit(t *testing.T) {
	tree := createTree(6, 4)

	items := []Item{}
	for i := 0; i < 5; i++ {
		_, item := assertTreeCanInsertAndFind(t, tree, i, fmt.Sprintf("item-%d", i))
		items = append(items, item)
	}

	assertTreeItemsAreSame(t, tree, items)

	if len(adapter.nodes) != 3 {
		t.Fatalf("Created an invalid amount of nodes: %d", len(adapter.nodes))
	}
}

func TestBPlusTree_RightSplitLeavesAttachedToRoot(t *testing.T) {
	tree := createTree(6, 4)
	totalEntries := 7*2 + 1

	items := []Item{}
	for i := 0; i < totalEntries; i++ {
		_, item := assertTreeCanInsertAndFind(t, tree, i, fmt.Sprintf("item-%d", i))
		items = append(items, item)
	}

	assertTreeItemsAreSame(t, tree, items)

	if len(adapter.nodes) != 8 {
		t.Fatalf("Created an invalid amount of nodes: %d", len(adapter.nodes))
	}
}

func TestBPlusTree_LeftSplitLeavesAttachedToRoot(t *testing.T) {
	tree := createTree(6, 4)
	totalEntries := 7*2 + 1

	items := []Item{}
	for i := totalEntries-1; i >= 0; i-- {
		_, item := assertTreeCanInsertAndFind(t, tree, i, fmt.Sprintf("item-%d", i))
		items = append([]Item{item}, items...)
	}

	assertTreeItemsAreSame(t, tree, items)

	if len(adapter.nodes) != 6 {
		t.Fatalf("Created an invalid amount of nodes: %d", len(adapter.nodes))
	}
}

func TestBPlusTree_SplitBranches(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1)*(branchCapacity/2+1)*leafCapacity/2 + 1

	items := []Item{}
	for i := 0; i < totalEntries; i++ {
		_, item := assertTreeCanInsertAndFind(t, tree, i+1, fmt.Sprintf("item-%d", i))
		items = append(items, item)
	}

	assertTreeItemsAreSame(t, tree, items)

	if len(adapter.nodes) != 36 {
		t.Fatalf("Created an invalid amount of nodes: %d", len(adapter.nodes))
	}
}

func TestBPlusTree_SplitsOnInternalNodes(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1) * leafCapacity

	for i := 0; i < totalEntries/2; i++ {
		key := i * 10
		insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
	}
	for i := 0; i < totalEntries/2; i++ {
		key := i * 10 + 1
		insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
	}
	for i := 0; i < totalEntries/4; i++ {
		key := i * 10 + 2
		insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
	}
	var lastKey Key
	tree.All(func (entry LeafEntry) {
		if lastKey == nil {
			lastKey = entry.Key
		} else if entry.Key.Less(lastKey) {
			t.Fatal("Items are not in order")
		}
		lastKey = entry.Key
	})
	if len(adapter.nodes) != 16 {
		t.Fatalf("Created an invalid amount of nodes: %d", len(adapter.nodes))
	}
}

func TestBPlusTree_MaximizesUtilization(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity + 1) * leafCapacity

	offset := 0
	for i := 1; i <= totalEntries/2+leafCapacity; i++ {
		key := offset + i*10
		assertTreeCanInsertAndFind(t, tree, key, fmt.Sprintf("item-%d", key))

		key = offset + i*10 + 1
		assertTreeCanInsertAndFind(t, tree, key, fmt.Sprintf("item-%d", key))
		offset += leafCapacity/2 + 1
	}
	offset = 0
	for i := 1; i <= totalEntries/2+leafCapacity/2; i++ {
		key := offset + i*10 + 2
		assertTreeCanInsertAndFind(t, tree, key, fmt.Sprintf("item-%d", key))

		key = offset + i*10 + 3
		assertTreeCanInsertAndFind(t, tree, key, fmt.Sprintf("item-%d", key))
		offset += leafCapacity/2 + 1
	}

	if len(adapter.nodes) != 22 {
		t.Fatalf("Created an unexpected set of nodes, total=%d, expected=22", len(adapter.nodes))
	}
}

func TestBPlusTree_LeafRootDelete(t *testing.T) {
	tree := createTree(6, 4)
	for i := 0; i < 4; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	for i := 0; i < 4; i++ {
		assertTreeCanDeleteByKey(t, tree, i)
	}
	for i := 0; i < 3; i++ {
		assertTreeCantFindByKey(t, tree, i)
	}
	if len(adapter.nodes) != 1 {
		t.Fatalf("Created an unexpected set of nodes, total=%d, expected=1", len(adapter.nodes))
	}
}

func TestBPlusTree_PipeItemsFromLeafSiblings(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := branchCapacity * leafCapacity/2

	for i := 0; i < totalEntries/2; i++ {
		key := i * 10
		insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
	}
	for i := 0; i < totalEntries/2; i++ {
		key := i * 10 + 1
		insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
	}

	nodesCount := len(adapter.nodes)

	// REFACTOR: Magic numbers sucks...
	assertTreeCanDeleteByKey(t, tree, 20)
	assertTreeCanDeleteByKey(t, tree, 30)

	if len(adapter.nodes) != nodesCount {
		t.Fatalf("Did not merge back nodes, total=%d, expected=%d", len(adapter.nodes), nodesCount)
	}

	assertTreeKeysAreOrdered(t, tree)
}

func TestBPlusTree_RightMergeLeavesAttachedToRoot(t *testing.T) {
	tree := createTree(6, 4)
	// REFACTOR: Magic numbers sucks...
	for i := 0; i < 6*2; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	for i := 0; i < 6*2; i++ {
		assertTreeCanDeleteByKey(t, tree, i)
	}

	if len(adapter.nodes) != 1 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=1", len(adapter.nodes))
	}
}

func TestBPlusTree_LeftMergeLeavesAttachedToRoot(t *testing.T) {
	tree := createTree(6, 4)
	// REFACTOR: Magic numbers sucks...
	for i := 0; i < 6*2; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	for i := 6*2-1; i >= 0; i-- {
		assertTreeCanDeleteByKey(t, tree, i)
	}

	if len(adapter.nodes) != 1 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=1", len(adapter.nodes))
	}
}

func TestBPlusTree_PipeItemsFromBranchSiblings(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1) * leafCapacity

	for r := 0; r < 4; r++ {
		for i := 0; i < totalEntries/2; i++ {
			key := i * 10 + r
			insertOnTree(t, tree, key, fmt.Sprintf("item-%d", key))
		}
	}

	// REFACTOR: Magic numbers sucks...
	// Here be dragons!
	assertTreeCanDeleteByKey(t, tree, 0)
	assertTreeCanDeleteByKey(t, tree, 1)
	assertTreeCanDeleteByKey(t, tree, 10)
	assertTreeCanDeleteByKey(t, tree, 11)
	assertTreeCanDeleteByKey(t, tree, 12)
	assertTreeCanDeleteByKey(t, tree, 13)
	assertTreeCanDeleteByKey(t, tree, 32)
	assertTreeCanDeleteByKey(t, tree, 33)
	assertTreeCanDeleteByKey(t, tree, 92)
	assertTreeCanDeleteByKey(t, tree, 93)
	assertTreeCanDeleteByKey(t, tree, 102)
	assertTreeCanDeleteByKey(t, tree, 103)

	nodesCount := len(adapter.nodes)

	assertTreeCanDeleteByKey(t, tree, 30)
	assertTreeCanDeleteByKey(t, tree, 101)

	if len(adapter.nodes) != nodesCount-2 {
		t.Fatalf("Did not pipe keys between branches back nodes, total=%d, expected=%d", len(adapter.nodes), nodesCount-2)
	}

	assertTreeKeysAreOrdered(t, tree)
}

func TestBPlusTree_RightMergeBranches(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1)*(branchCapacity/2+1)*leafCapacity/2

	for i := 0; i < totalEntries; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	nodesBefore := len(adapter.nodes)
	assertTreeCanDeleteByKey(t, tree, 0)

	if nodesBefore - len(adapter.nodes) != 2 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=%d", len(adapter.nodes), nodesBefore-2)
	}

	assertTreeKeysAreOrdered(t, tree)
}

func TestBPlusTree_LeftMergeBranches(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1)*(branchCapacity/2+1)*leafCapacity/2

	for i := 0; i < totalEntries-2; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	nodesBefore := len(adapter.nodes)
	// REFACTOR: Magic numbers sucks...
	for i := 47; i <= 53; i++ {
		assertTreeCanDeleteByKey(t, tree, i)
	}

	if nodesBefore - len(adapter.nodes) != 4 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=%d", len(adapter.nodes), nodesBefore-4)
	}

	assertTreeKeysAreOrdered(t, tree)
}

func TestBPlusTree_RightMergeBranchesUpToRoot(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := branchCapacity*branchCapacity*leafCapacity

	for i := 0; i < totalEntries; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	for i := 0; i < totalEntries; i++ {
		assertTreeCanDeleteByKey(t, tree, i)
	}

	if len(adapter.nodes) != 1 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=%d", len(adapter.nodes), 1)
	}
}

func TestBPlusTree_LeftMergeBranchesUpToRoot(t *testing.T) {
	branchCapacity := 6
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := branchCapacity*branchCapacity*leafCapacity

	for i := 0; i < totalEntries; i++ {
		insertOnTree(t, tree, i, fmt.Sprintf("item-%d", i))
	}
	for i := totalEntries-1; i >= 0; i-- {
		assertTreeCanDeleteByKey(t, tree, i)
	}

	if len(adapter.nodes) != 1 {
		t.Fatalf("Did not merge back nodes, total=%d, expected=%d", len(adapter.nodes), 1)
	}
}

func TestBPlusTree_GrowAndShrinkLotsOfEntriesTwice(t *testing.T) {
	branchCapacity := 4
	leafCapacity := 4
	tree := createTree(branchCapacity, leafCapacity)
	totalEntries := (branchCapacity+1)*leafCapacity

	keys := make([]int, 0, totalEntries*30)
	for h := 0; h < 30; h++ {
		var start, end int
		if h % 2 == 0 {
			start = 0
			end = totalEntries/2
		} else {
			start = totalEntries/2+1
			end = totalEntries
		}
		for i := start; i < end; i++ {
			key := i * 50 + h
			assertTreeCanInsertAndFind(t, tree, key, fmt.Sprintf("item-%d", key))
			keys = append(keys, key)
			assertTreeKeysAreOrdered(t, tree)
		}
	}
}

func createTree(branchCapacity int, leafCapacity int) BPlusTree {
	adapter = newInMemoryAdapter()
	tree := New(Config{
		Adapter:        adapter,
		LeafCapacity:   leafCapacity,
		BranchCapacity: branchCapacity,
	})
	return tree
}

func insertOnTree(t *testing.T, tree BPlusTree, intKey int, stringItem string) {
	key := Uint32Key(intKey)
	item := StringItem(stringItem)
	if err := tree.Insert(key, item); err != nil {
		t.Fatalf("Error inserting item with key %d: %s", key, err)
	}
}

func assertTreeCanDeleteByKey(t *testing.T, tree BPlusTree, intKey int) {
	key := Uint32Key(intKey)
	tree.Delete(key)
	assertTreeCantFindByKey(t, tree, intKey)
}

func assertTreeCantFindByKey(t *testing.T, tree BPlusTree, intKey int) {
	key := Uint32Key(intKey)
	if _, err := tree.Find(key); err == nil {
		t.Error("Did not remove key from tree")
	}
}

func assertTreeCanInsertAndFind(t *testing.T, tree BPlusTree, intKey int, stringItem string) (Uint32Key, StringItem) {
	insertOnTree(t, tree, intKey, stringItem)
	itemFound, err := tree.Find(Uint32Key(intKey))
	if err != nil {
		t.Fatalf("Error when trying to find item with key=%+v: %s", intKey, err)
	}
	if itemFound == nil {
		t.Errorf("Could not retrieve %d from tree right after inserting it", intKey)
	}
	if itemFound != StringItem(stringItem) {
		t.Errorf("Invalid value returned from the tree: %+v", itemFound)
	}
	return Uint32Key(intKey), StringItem(stringItem)
}

func assertTreeItemsAreSame(t *testing.T, tree BPlusTree, items []Item) {
	i := 0
	funcCalled := false
	tree.All(func(entry LeafEntry) {
		funcCalled = true
		if entry.Item != items[i] {
			t.Errorf("Invalid value returned from the tree at %d: %+v (expected %+v)", i, entry.Item, items[i])
		}
		i++
	})
	if i != len(items) {
		t.Errorf("Did not iterate over all entries")
	}
	if !funcCalled {
		t.Errorf("Function passed to BPlusTree was not called")
	}
}

func assertTreeKeysAreOrdered(t *testing.T, tree BPlusTree) {
	var lastKey Key
	tree.All(func (entry LeafEntry) {
		if lastKey == nil {
			lastKey = entry.Key
		} else if entry.Key.Less(lastKey) {
			t.Fatal("Items are not in order")
		}
		lastKey = entry.Key
	})
}
