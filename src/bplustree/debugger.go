package bplustree

import (
	"fmt"
	"regexp"
)

func DumpTree(tree BPlusTree, adapter NodeAdapter) string {
	root := adapter.LoadRoot()
	return fmt.Sprint(dumpNode(tree, adapter, "", root))
}

func dumpNode(tree BPlusTree, adapter NodeAdapter, indent string, node Node) string {
	if node == nil {
		return "EMPTY"
	}

	if leafRoot, isLeaf := node.(LeafNode); isLeaf {
		return dumpLeaf(tree, adapter, indent, leafRoot)
	} else {
		branchRoot, _ := node.(BranchNode)
		return dumpBranch(tree, adapter, indent, branchRoot)
	}
}

func dumpLeaf(tree BPlusTree, adapter NodeAdapter, indent string, leaf LeafNode) string {
	keys := []Key{}
	leaf.All(func(entry LeafEntry) {
		keys = append(keys, entry.Key)
	})
	return fmt.Sprintf(indent+"LEAF (ID=%d, parentID=%d, left=%d, right=%d) %+v\n", leaf.ID(), leaf.ParentID(), leaf.LeftSiblingID(), leaf.RightSiblingID(), keys)
}

func dumpBranch(tree BPlusTree, adapter NodeAdapter, indent string, branch BranchNode) string {
	output := fmt.Sprintf(indent+"BRANCH (ID=%d, parentID=%d, left=%d, right=%d)\n", branch.ID(), branch.ParentID(), branch.LeftSiblingID(), branch.RightSiblingID())
	re := regexp.MustCompile("(.)")
	indent = re.ReplaceAllString(indent, " ")
	i := 0
	total := branch.TotalKeys()
	branch.All(func(entry BranchEntry) {
		i += 1
		ltNode := adapter.LoadNode(entry.LowerThanKeyNodeID)
		childIndent := fmt.Sprintf("%s [<  %2d]", indent, entry.Key)
		output += dumpNode(tree, adapter, childIndent, ltNode)
		if i == total {
			gteNode := adapter.LoadNode(entry.GreaterThanOrEqualToKeyNodeID)
			childIndent := fmt.Sprintf("%s [>= %2d]", indent, entry.Key)
			output += dumpNode(tree, adapter, childIndent, gteNode)
		}
	})
	return output
}
