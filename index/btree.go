package index

import (
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/luminocean/gled/page"
	"io"
	"os"
	"unsafe"
)

var (
	// size of a page header
	btreeHeaderSize = uint32(unsafe.Sizeof(BTreeHeader{}))
)

type BTreeHeader struct {
	// index of the root page
	// -1 means root is not allocated yet
	rootIdx int64
}

type BTree struct {
	// Data file
	data        *os.File
	header      BTreeHeader
	initialized bool
}

func NewBTree(data *os.File) *BTree {
	return &BTree{
		data: data,
	}
}

func (t *BTree) Init() (err error) {
	err = t.readHeader()
	if err != nil {
		return
	}
	t.initialized = true
	return
}

func (t *BTree) Insert(tuple page.Tuple, key exp.OpValue) (err error) {
	if !t.initialized {
		err = t.Init()
		if err != nil {
			return
		}
	}
	if t.header.rootIdx == -1 {
		// no page yet, create the initial one
		var idx int64
		idx, err = t.allocateNewPage()
		if err != nil {
			return
		}
		if idx != 0 {
			err = fmt.Errorf("initial page index is not zero")
			return
		}
	}
	err = t.insertToPage(t.header.rootIdx, tuple, key)
	if err != nil {
		return
	}
	return
}

func (t *BTree) insertToPage(idx int64, tuple page.Tuple, key exp.OpValue) (err error) {
	// TODO
	return
}

func (t *BTree) allocateNewPage() (idx int64, err error) {
	// TODO: lazy allocation?
	// seek to the end of the data file and add a new, yet empty page there
	offset, err := t.data.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}
	emptyData := make([]byte, page.PageSize)
	_, err = t.data.Write(emptyData)
	if err != nil {
		err = fmt.Errorf("failed to write new FSM byte: %w", err)
		return
	}
	if offset%page.PageSize != 0 {
		err = fmt.Errorf("data file offset %d is not multiples of the page size %d", offset, page.PageSize)
		return
	}
	idx = offset / page.PageSize
	return
}

func (t *BTree) readHeader() (err error) {
	buffer := make([]byte, btreeHeaderSize)
	err = t.readAt(buffer, 0)
	if err != nil {
		if err == io.EOF {
			// empty tree file, mark the root index as -1 as non-existent
			t.header.rootIdx = -1
			err = nil
			return
		}
		return fmt.Errorf("failed to read page header: %w", err)
	}
	t.header.rootIdx = int64(page.Endian.Uint32(buffer[0:4]))
	return
}

func (t *BTree) readAt(data []byte, position uint32) (err error) {
	_, err = t.data.Seek(int64(position), io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek the page Data: %w", err)
		return
	}
	read, err := t.data.Read(data)
	if err != nil {
		// for EOF, return as is since it might be normal sometimes
		if err == io.EOF {
			return err
		}
		err = fmt.Errorf("failed to read Data from Data: %w", err)
		return
	} else if read != len(data) {
		err = fmt.Errorf("mismatched number of bytes read")
		return
	}
	return
}
