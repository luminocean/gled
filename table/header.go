package table

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/luminocean/gled/util"
	"unsafe"
)

var (
	// size of a page header
	pageHeaderSize = uint32(unsafe.Sizeof(PageHeader{}))
	// size of a page pointer
	pagePointerSize = uint32(unsafe.Sizeof(PagePointer(0)))
)

// PageHeader is the head of a page
type PageHeader struct {
	// Lower is the starting position of the free space (inclusive)
	// relative to the containing page offset
	lower PagePointer
	// Lower is the ending position of the free space (exclusive)
	// relative to the containing page offset
	upper PagePointer
}

func (h *PageHeader) toBytes() (data []byte) {
	buffer := bytes.Buffer{}
	w := bufio.NewWriter(&buffer)
	_, err := w.Write(util.Uint32ToBytes(uint32(h.lower)))
	if err != nil {
		panic(err) // in-memory write, should not fail
	}
	_, err = w.Write(util.Uint32ToBytes(uint32(h.upper)))
	if err != nil {
		panic(err)
	}
	err = w.Flush()
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func (h *PageHeader) fromBytes(data []byte) (err error) {
	if uint32(len(data)) != pageHeaderSize {
		err = fmt.Errorf("wrong file size for page header: expect %d but got %d", pageHeaderSize, len(data))
		return
	}
	var offset uint32 = 0
	h.lower = PagePointer(util.Endian.Uint32(data[offset : offset+pagePointerSize]))
	offset += pagePointerSize
	h.upper = PagePointer(util.Endian.Uint32(data[offset : offset+pagePointerSize]))
	offset += pagePointerSize
	return
}
