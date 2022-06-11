package index

import (
	"github.com/luminocean/gled/exp"
	"os"
)

const (
	NodeSizeLimit = 1024 * 8
	// NodeItemSizeLimit needs to ensure the split nodes are not full
	// otherwise there will be endless splitting
	// the size limit 2048 ensures that there can be at least two items contained in one node
	NodeItemSizeLimit = 2048
)

type Node interface {
	// Fit checks whether the node can accommodate the incoming values
	Fit(key exp.OpValue, payload []byte) (ok bool)
	// Insert inserts a key-value pair or a branch into the node
	// Note that the node must have enough free space to accommodate the key value pair
	Insert(key exp.OpValue, payload []byte) (err error)
	// Lookup looks up the given key and returns related payload
	Lookup(key exp.OpValue) (payload []byte, exists bool, err error)
	// Split splits (almost) half of the node data to another node
	Split() (middleKey exp.OpValue, right Node, err error)
}

type NodeSrc struct {
	file   *os.File
	offset int64
}

func search[T exp.OpValue](keys []T, target T) (exists bool, idx int, err error) {
	// locate the key in the items if exists
	li, ri := 0, len(keys)-1
	for li <= ri {
		mi := li + (ri-li)/2
		var cmp int
		cmp, err = keys[mi].Compare(target)
		if err != nil {
			return
		}
		if cmp == 0 {
			idx, exists = mi, true
			return
		} else if cmp < 0 {
			li = mi + 1
		} else {
			ri = mi - 1
		}
	}
	// the next idx to insert
	idx = li
	if ri > li {
		idx = ri
	}
	exists = false
	return
}

func insert[T any](slice []T, target T, idx int) []T {
	// no existing key found, now we need to insert the key at idx
	// right shift items by 1 to make room for the new item
	slice = append(slice, target)
	for i := len(slice) - 1; i >= idx && i > 0; i-- {
		slice[i] = slice[i-1]
	}
	slice[idx] = target
	return slice
}
