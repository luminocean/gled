package index

import (
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/luminocean/gled/util"
	"os"
)

/*
	File layout:
	- header
	- node pages

	Header:
	- root index (uint32)
	- root type (uint8 - 0: leaf node, 1: internal node)
*/

const (
	RootIndexSize = 4
	RootTypeSize  = 1
)

type BTree struct {
	file        *os.File
	root        Node
	initialized bool
}

func NewBTree(file *os.File) *BTree {
	return &BTree{
		file: file,
	}
}

func (t *BTree) Initialize() (err error) {
	// read btree header
	buffer := make([]byte, 5)
	err = util.ReadAt(t.file, buffer, 0)
	if err != nil {
		return
	}
	rootIdx := util.BytesToUint32(buffer[:RootIndexSize])
	rootOffset := int64(RootIndexSize + RootTypeSize + NodeSizeLimit*rootIdx)
	rootType := util.BytesToUint32(buffer[RootIndexSize : RootIndexSize+RootTypeSize])
	if rootType == 0 {
		t.root = NewLeafNode(t.file, rootOffset)
	} else if rootType == 1 {
		t.root = NewInternalNode(t.file, rootOffset)
	}
	t.initialized = true
	return
}

func (t *BTree) Insert(key exp.OpValue, value []byte) (err error) {
	if !t.initialized {
		err = t.Initialize()
		if err != nil {
			return
		}
	}
	// no root yet, allocate one
	if t.root == nil {
		t.root, err = AllocateNewLeafNode(t.file)
		if err != nil {
			return
		}
	}
	// if root has room for new kv
	if t.root.Fit(key, value) {
		err = t.root.Insert(key, value)
		if err != nil {
			err = fmt.Errorf("failed to insert into the btree node: %w", err)
			return
		}
		return
	}
	// otherwise, split root
	middleKey, right, err := t.root.Split()
	if err != nil {
		return
	}
	left := t.root
	newRoot, err := AllocateNewInternalNode(t.file)
	if err != nil {
		return
	}
	err = newRoot.Expand(middleKey, left, right)
	if err != nil {
		err = fmt.Errorf("failed to insert splitted nodes into the parent node: %w", err)
		return
	}
	t.root = newRoot
	return
}
