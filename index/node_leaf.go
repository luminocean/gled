package index

import (
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/luminocean/gled/util"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"os"
	"unsafe"
)

/*
	File layout:
	- header
	- items

	Header:
	- item count (uint32)

	Item:
	- key size (uint32)
	- payload size (uint32)
	- key
	- payload
*/

type LeafNodeItem struct {
	Key     exp.OpValue
	Payload []byte
}

func (item *LeafNodeItem) Size() uint32 {
	return uint32(unsafe.Sizeof(item.Key)) + uint32(len(item.Payload))
}

type LeafNode struct {
	src   *NodeSrc
	items []LeafNodeItem
	// track item total size in memory
	size        uint32
	initialized bool
}

func NewLeafNode(file *os.File, offset int64) *LeafNode {
	return &LeafNode{
		src: &NodeSrc{
			file:   file,
			offset: offset,
		},
	}
}

func AllocateNewLeafNode(file *os.File) (node *LeafNode, err error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}
	// TODO: lazy allocation?
	content := make([]byte, NodeSizeLimit)
	err = util.WriteAt(file, content, offset)
	if err != nil {
		return
	}
	node = NewLeafNode(file, offset)
	return
}

func (n *LeafNode) Initialize() (err error) {
	buffer := make([]byte, 4)
	_, err = n.src.file.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	// read the node size
	byteCount, err := n.src.file.Read(buffer)
	if err != nil {
		return
	}
	if byteCount != 4 {
		err = fmt.Errorf("invalid number of bytes for node size: %d", byteCount)
		return
	}
	size := util.BytesToUint32(buffer)
	if size > NodeSizeLimit {
		err = fmt.Errorf("invalid node size: %d", size)
		return
	}
	n.size = size
	if size == 0 {
		return
	}

	// read the content
	buffer = make([]byte, size)
	byteCount, err = n.src.file.Read(buffer)
	if err != nil {
		return
	}
	if uint32(byteCount) != size {
		err = fmt.Errorf("invalid number of bytes for node content: %d", byteCount)
		return
	}
	err = msgpack.Unmarshal(buffer, &n.items)
	if err != nil {
		err = fmt.Errorf("failed to read node content: %w", err)
		return
	}
	n.initialized = true
	return
}

func (n *LeafNode) Fit(key exp.OpValue, payload []byte) bool {
	item := LeafNodeItem{
		Key:     key,
		Payload: payload,
	}
	return n.size+item.Size() <= NodeSizeLimit
}

func (n *LeafNode) Insert(key exp.OpValue, payload []byte) (err error) {
	if !n.initialized {
		err = n.Initialize()
		if err != nil {
			return
		}
	}
	if !n.Fit(key, payload) {
		err = fmt.Errorf("no room for new kv insertion")
		return
	}
	exists, idx, err := search(n.keys(), key)
	if err != nil {
		return
	}
	if exists {
		// found the same key, update its value and done
		n.size -= uint32(len(n.items[idx].Payload))
		n.items[idx].Payload = payload
		n.size += uint32(len(payload))
		return
	}
	item := LeafNodeItem{
		Key:     key,
		Payload: payload,
	}
	n.items = insert(n.items, item, idx)
	n.size += item.Size()
	return
}

func (n *LeafNode) Lookup(key exp.OpValue) (payload []byte, exists bool, err error) {
	var idx int
	exists, idx, err = search(n.keys(), key)
	if err != nil {
		return
	}
	if exists {
		payload = n.items[idx].Payload
		return
	}
	return
}

// Split splits a leaf node into two leaves
func (n *LeafNode) Split() (middleKey exp.OpValue, right Node, err error) {
	if len(n.items) <= 1 {
		panic("not enough items to split")
	}
	// find the split point i (starting point of the right split) where count(left) >= count(right)
	var i, acc = 0, 0
	for ; i < len(n.items); i++ {
		if acc <= NodeSizeLimit/2 {
			acc += int(n.items[i].Size())
		} else {
			break
		}
	}
	if i == len(n.items) {
		panic("no item for the right split")
	}
	middleKey = n.items[i].Key
	rl, err := AllocateNewLeafNode(n.src.file)
	if err != nil {
		return
	}
	// moving nodes from left to right
	for ; i < len(n.items); i++ {
		err = rl.Insert(n.items[i].Key, n.items[i].Payload)
		if err != nil {
			return
		}
	}
	right = rl
	// remove moved nodes in the left split
	n.items = n.items[:i]
	return
}

func (n *LeafNode) Flush() (err error) {
	data, err := msgpack.Marshal(n.items)
	if err != nil {
		return
	}
	size := uint32(len(data))
	// seek to the part for the node in the file
	_, err = n.src.file.Seek(n.src.offset, io.SeekStart)
	if err != nil {
		return
	}
	// write the size
	_, err = n.src.file.Write(util.Uint32ToBytes(size))
	if err != nil {
		return
	}
	// then write the real data
	_, err = n.src.file.Write(data)
	if err != nil {
		return
	}
	// flush
	err = n.src.file.Sync()
	if err != nil {
		return
	}
	return
}

func (n *LeafNode) keys() []exp.OpValue {
	keys := make([]exp.OpValue, len(n.items))
	for i, item := range n.items {
		keys[i] = item.Key
	}
	return keys
}
