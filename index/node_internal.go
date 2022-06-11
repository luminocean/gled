package index

import (
	"github.com/luminocean/gled/exp"
	"math"
	"os"
	"unsafe"
)

type InternalNode struct {
	src  *NodeSrc
	keys []exp.OpValue
	// len(slots) == len(keys) + 1 when keys and slots are not empty
	slots []Node
}

func AllocateNewInternalNode(file *os.File) (node *InternalNode, err error) {
	panic("implement me")
}

func NewInternalNode(file *os.File, offset int64) *InternalNode {
	return &InternalNode{
		src: &NodeSrc{
			file:   file,
			offset: offset,
		},
	}
}

func (n *InternalNode) Fit(key exp.OpValue, payload []byte) (ok bool) {
	// note that payload is not used since an internal node doesn't store payload
	delta := int(unsafe.Sizeof(key))
	if delta > math.MaxInt32 {
		panic("key too large")
	}
	panic("implement me")
	return true
}

func (n *InternalNode) Insert(key exp.OpValue, payload []byte) (err error) {
	slotIdx, err := n.locateSlot(key)
	if err != nil {
		return
	}
	// node to insert
	node := n.slots[slotIdx]
	// if the node doesn't have enough room, split it
	if !node.Fit(key, payload) {
		mk, right, err := node.Split()
		if err != nil {
			return err
		}
		// update the internal node with split sub nodes
		left := node
		err = n.Expand(mk, left, right)
		if err != nil {
			return err
		}
		// decide to go left or right
		var cmp int
		cmp, err = key.Compare(mk)
		if err != nil {
			return err
		}
		if cmp <= 0 {
			node = left
		} else {
			node = right
		}
	}
	// all clear, insert
	err = node.Insert(key, payload)
	if err != nil {
		return
	}
	return
}

func (n *InternalNode) Lookup(key exp.OpValue) (payload []byte, exists bool, err error) {
	slotIdx, err := n.locateSlot(key)
	if err != nil {
		return
	}
	node := n.slots[slotIdx]
	return node.Lookup(key)
}

// Expand adds a new key (and its adjacent slots) to an internal node
func (n *InternalNode) Expand(key exp.OpValue, left Node, right Node) (err error) {
	exists, idx, err := search(n.keys, key)
	if err != nil {
		return
	}
	if exists {
		// update left and right
		n.slots[idx] = left
		n.slots[idx+1] = right
		return
	}
	// insert the new key
	n.keys = insert(n.keys, key, idx)

	// and slots
	if len(n.slots) == 0 {
		// empty slot, just add one
		n.slots = append(n.slots, left)
	} else {
		// reuse an old slot
		n.slots[idx] = left
	}
	n.slots = insert(n.slots, right, idx+1) // insert a new one
	return
}

func (n *InternalNode) Split() (middleKey exp.OpValue, right Node, err error) {
	if len(n.keys) <= 3 {
		panic("too few keys to split")
	}
	// find the split point i (which becomes the middle key)
	var i, acc = 0, 0
	for ; i < len(n.keys); i++ {
		if acc <= NodeSizeLimit/2 {
			acc += int(unsafe.Sizeof(n.keys[i]) + unsafe.Sizeof(n.slots[i]))
		} else {
			break
		}
	}
	if i == len(n.keys) {
		panic("no item for the right split")
	}
	middleKey = n.keys[i]
	rl, err := AllocateNewInternalNode(n.src.file)
	if err != nil {
		return
	}
	// moving nodes from left to right
	var j = i + 1
	for ; j < len(n.keys); j++ {
		rl.keys = append(rl.keys, n.keys[j])
		rl.slots = append(rl.slots, n.slots[j])
	}
	// move the tailing slot as well
	rl.slots = append(rl.slots, n.slots[j])
	right = rl
	// remove moved keys and slots in the left split
	n.keys = n.keys[:i]
	n.slots = n.slots[:i+1]
	return
}

// locate the slot for insertion/lookup
func (n *InternalNode) locateSlot(key exp.OpValue) (slotIdx int, err error) {
	_, keyIdx, err := search(n.keys, key)
	if err != nil {
		return
	}
	if keyIdx == len(n.keys) {
		// key is larger than all existing keys, pointing to a non-existent key
		// use the last slot
		slotIdx = len(n.slots) - 1
	}
	// 1. found an exact match, go the left branch of the matched key
	// 2. not found, go to the left branch of the closest key (largest but smaller than key)
	slotIdx = keyIdx
	return
}
