package index

import (
	"errors"
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/luminocean/gled/util"
	"io"
	"os"
)

/*
	File layout:
	- header
	- items

	Header:
	- item count (uint32)

	Item:
	- key type (uint8)
	- key size (uint32)
	- key
	- payload size (uint32)
	- payload
*/

const (
	LeafNodeHeaderSize = 4
)

type LeafNode struct {
	src   *NodeSrc
	items LeafNodeItems
	// total size of items, excluding the header
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
	_, err = n.src.file.Seek(n.src.offset, io.SeekStart)
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
	n.initialized = true
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
	n.items, err = n.items.Unmarshal(buffer)
	if err != nil {
		err = fmt.Errorf("failed to read node content: %w", err)
		return
	}
	return
}

func (n *LeafNode) Size() uint32 {
	return LeafNodeHeaderSize + n.items.Size()
}

func (n *LeafNode) Fit(key exp.OpValue, payload []byte) bool {
	newItem := LeafNodeItem{
		Key:     key,
		Payload: payload,
	}
	return n.Size()+newItem.Size() <= NodeSizeLimit
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
		n.items[idx].Payload = payload
		return
	}
	item := LeafNodeItem{
		Key:     key,
		Payload: payload,
	}
	n.items = insert(n.items, item, idx)
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
	var si, acc = 0, 0
	for ; si < len(n.items); si++ {
		if acc <= NodeSizeLimit/2 {
			acc += int(n.items[si].Size())
		} else {
			break
		}
	}
	if si == len(n.items) {
		panic("no item for the right split")
	}
	middleKey = n.items[si].Key
	rl, err := AllocateNewLeafNode(n.src.file)
	if err != nil {
		return
	}
	// moving nodes from left to right
	for i := si; i < len(n.items); i++ {
		err = rl.Insert(n.items[i].Key, n.items[i].Payload)
		if err != nil {
			return
		}
	}
	right = rl
	// remove moved nodes in the left split
	n.items = n.items[:si]
	return
}

func (n *LeafNode) Flush() (err error) {
	data := n.items.Marshal()
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

type LeafNodeItem struct {
	Key     exp.OpValue
	Payload []byte
}

func (item *LeafNodeItem) Size() uint32 {
	var keySize uint32
	switch item.Key.(type) {
	case exp.String:
		keySize = uint32(len(item.Key.(exp.String)))
	case exp.Int32:
		keySize = 4
	case exp.Int64:
		keySize = 8
	case exp.Float32:
		keySize = 4
	case exp.Float64:
		keySize = 8
	default:
		panic("undefined exp type")
	}
	payloadSize := uint32(len(item.Payload))
	// 1 + 4 + 4 is metadata size, see Marshal()
	return 1 + 4 + 4 + keySize + payloadSize
}

func (item *LeafNodeItem) Marshal() (data []byte) {
	// uint8 - key type
	// 0 is saved for undefined
	var keyType uint8
	// uint32 - key size (because string is variant)
	var keySize uint32
	// bytes for the key
	var keyData []byte
	switch item.Key.(type) {
	case exp.String:
		keyType = 1
		keySize = uint32(len(item.Key.(exp.String)))
		keyData = []byte(item.Key.(exp.String))
	case exp.Int32:
		keyType = 2
		keySize = 4
		keyData = util.Uint32ToBytes(uint32(item.Key.(exp.Int32)))
	case exp.Int64:
		keyType = 3
		keySize = 8
		keyData = util.Uint64ToBytes(uint64(item.Key.(exp.Int64)))
	case exp.Float32:
		keyType = 4
		keySize = 4
		keyData = util.Float32ToBytes(float32(item.Key.(exp.Float32)))
	case exp.Float64:
		keyType = 5
		keySize = 8
		keyData = util.Float64ToBytes(float64(item.Key.(exp.Float64)))
	default:
		panic("undefined exp type")
	}
	data = append(data, keyType)
	data = append(data, util.Uint32ToBytes(keySize)...)
	data = append(data, keyData...)

	// uint32 - payload size
	data = append(data, util.Uint32ToBytes(uint32(len(item.Payload)))...)
	// bytes for the entire payload
	data = append(data, item.Payload...)
	return
}

func (item *LeafNodeItem) Unmarshal(data []byte) (read int, err error) {
	offset := 0
	// uint8 - key type
	keyType := data[offset]
	offset += 1

	var keyData exp.OpValue
	switch keyType {
	case 1:
		keySize := int(util.BytesToUint32(data[offset : offset+4]))
		offset += 4
		keyData = exp.String(string(data[offset : offset+keySize]))
		offset += keySize
	case 2:
		keySize := int(util.BytesToUint32(data[offset : offset+4]))
		offset += 4
		if keySize != 4 {
			panic("wrong size for int32")
		}
		keyData = exp.Int32(util.BytesToUint32(data[offset : offset+4]))
		offset += 4
	case 3:
		keySize := int(util.BytesToUint64(data[offset : offset+8]))
		offset += 8
		if keySize != 8 {
			panic("wrong size for int64")
		}
		keyData = exp.Int64(util.BytesToUint64(data[offset : offset+8]))
		offset += 8
	case 4:
		keySize := int(util.BytesToFloat32(data[offset : offset+4]))
		offset += 4
		if keySize != 4 {
			panic("wrong size for float32")
		}
		keyData = exp.Int32(util.BytesToFloat32(data[offset : offset+4]))
		offset += 4
	case 5:
		keySize := int(util.BytesToFloat64(data[offset : offset+8]))
		offset += 8
		if keySize != 8 {
			panic("wrong size for float64")
		}
		keyData = exp.Int64(util.BytesToFloat64(data[offset : offset+8]))
		offset += 8
	default:
		panic("undefined exp type")
	}
	item.Key = keyData

	// payload size
	payloadSize := int(util.BytesToUint32(data[offset : offset+4]))
	offset += 4
	item.Payload = data[offset : offset+payloadSize]
	offset += payloadSize

	read = offset
	return
}

type LeafNodeItems []LeafNodeItem

func (items LeafNodeItems) Size() uint32 {
	var itemsSize uint32
	for _, item := range items {
		itemsSize += item.Size()
	}
	// 4 is metadata, see Marshal()
	return 4 + itemsSize
}

func (items LeafNodeItems) Marshal() (data []byte) {
	// uint32 - size of items
	data = append(data, util.Uint32ToBytes(uint32(len(items)))...)
	for _, item := range items {
		var itemData []byte
		itemData = item.Marshal()
		// adding data from individual items
		data = append(data, itemData...)
	}
	return
}

func (items LeafNodeItems) Unmarshal(data []byte) (unmarshalled LeafNodeItems, err error) {
	offset := 0
	size := util.BytesToUint32(data[offset : offset+4])
	offset += 4
	// read items
	var i uint32
	for i = 0; i < size; i++ {
		item := LeafNodeItem{}
		var read int
		read, err = item.Unmarshal(data[offset:])
		if err != nil {
			return
		}
		offset += read
		unmarshalled = append(unmarshalled, item)
	}
	if offset != len(data) {
		err = errors.New("found remaining data while the unmarshalling is done")
		return
	}
	return
}
