package gled

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

// The page layout is borrowed from https://www.interdb.jp/pg/pgsql01.html

const (
	// size of one page
	pageSize = 1024 * 8
)

var (
	// endian for all data bytes in a page
	endian = binary.BigEndian
	// size of a page header
	pageHeaderSize = uint32(unsafe.Sizeof(PageHeader{}))
	// size of a page pointer
	pagePointerSize = uint32(unsafe.Sizeof(PagePointer(0)))
	// size of a tuple pointer
	tuplePointerSize = uint32(unsafe.Sizeof(TuplePointer{}))
)

// PagePointer is a pointer pointing to a location within a page
type PagePointer uint32

// PageHeader is the head of a page
type PageHeader struct {
	// Lower is the starting position of the free space (inclusive)
	// relative to the containing page offset
	lower PagePointer
	// Lower is the ending position of the free space (exclusive)
	// relative to the containing page offset
	upper PagePointer
}

func (p *PageHeader) toBytes() (data []byte) {
	buffer := bytes.Buffer{}
	w := bufio.NewWriter(&buffer)
	_, err := w.Write(uint32ToBytes(uint32(p.lower)))
	if err != nil {
		panic(err) // in-memory write, should not fail
	}
	_, err = w.Write(uint32ToBytes(uint32(p.upper)))
	if err != nil {
		panic(err)
	}
	err = w.Flush()
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

// postgres equivalent:
// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/itemid.h#L38
type tupleAttributes struct {
	// whether the pointer is in use
	used bool
}

// TuplePointer points to a tuple within a page
type TuplePointer struct {
	attrs   tupleAttributes
	dataPtr PagePointer
}

func NewTuplePointerFromBytes(data []byte) (pointer TuplePointer, err error) {
	if uint32(len(data)) != tuplePointerSize {
		err = fmt.Errorf("invalid number of bytes to create a tuple pointer")
		return
	}
	used := data[0]
	if used == 0 {
		pointer.attrs.used = false
	} else if used == 1 {
		pointer.attrs.used = true
	} else {
		err = fmt.Errorf("invalid pointer used attr: %d", used)
		return
	}
	pointer.dataPtr = PagePointer(bytesToUint32(data[1:]))
	return
}

func (p *TuplePointer) toBytes() (data []byte) {
	buffer := bytes.Buffer{}
	w := bufio.NewWriter(&buffer)
	var err error
	if p.attrs.used {
		_, err = w.Write([]byte{1})
	} else {
		_, err = w.Write([]byte{0})
	}
	if err != nil {
		panic(err) // in-memory write, should not fail
	}
	_, err = w.Write(uint32ToBytes(uint32(p.dataPtr)))
	if err != nil {
		panic(err)
	}
	err = w.Flush()
	if err != nil {
		panic(err)
	}
	data = buffer.Bytes()
	return
}

// Tuple is a data tuple
type Tuple []byte

func (t *Tuple) Size() uint32 {
	return uint32(len(*t))
}

// Page is a fixed-length area on a data to store tuples and related data structures
type Page struct {
	header      PageHeader
	data        *os.File
	offset      uint64
	initialized bool
}

// NewPage creates and initializes a new page
// from a specific offset of a data
func NewPage(data *os.File, offset uint64) *Page {
	return &Page{
		header:      PageHeader{},
		data:        data,
		offset:      offset,
		initialized: false,
	}
}

func (p *Page) Init() (err error) {
	err = p.readHeader()
	if err != nil {
		return
	}
	p.initialized = true
	return
}

// Add adds a tuple to the page
// returns the remaining free spaces for more tuples
func (p *Page) Add(tuple Tuple) (free uint32, err error) {
	if !p.initialized {
		err = p.Init()
		if err != nil {
			return
		}
	}
	if uint32(p.header.lower)+tuplePointerSize+tuple.Size() >= uint32(p.header.upper) {
		err = errors.New("no room for more tuples")
		return
	}

	// write the tuple
	tupleStart := uint32(p.header.upper) - tuple.Size()
	err = p.writeAt(tuple, tupleStart)
	if err != nil {
		return
	}
	// update upper
	p.header.upper = PagePointer(uint32(p.header.upper) - tuple.Size())

	// write the pointer for the tuple
	pointerStart := uint32(p.header.lower)
	pointer := TuplePointer{
		attrs: tupleAttributes{
			used: true,
		},
		dataPtr: PagePointer(tupleStart),
	}
	err = p.writeAt(pointer.toBytes(), pointerStart)
	if err != nil {
		return
	}
	// update lower
	p.header.lower = PagePointer(uint32(p.header.lower) + tuplePointerSize)

	// write updated lower and upper pointers
	err = p.writeAt(p.header.toBytes(), 0)
	if err != nil {
		return
	}

	// flush
	err = p.data.Sync()
	if err != nil {
		return
	}

	// the remaining hole size - one pointer
	free = uint32(p.header.upper) - uint32(p.header.lower) - pagePointerSize
	return
}

// Remove removes a tuple by providing the pointer index (starting from 0) pointing to the tuple
func (p *Page) Remove(tpIdx uint32) (err error) {
	pointerCount, err := p.countTuplePointers()
	if err != nil {
		err = fmt.Errorf("failed to count tuple pointers: %w", err)
		return
	}
	if tpIdx >= pointerCount {
		err = fmt.Errorf("tuple pointer index too large")
		return
	}
	// read the pointer from the data
	tpStart := pageHeaderSize + tuplePointerSize*tpIdx
	buffer := make([]byte, tuplePointerSize)
	err = p.readAt(buffer, tpStart)
	if err != nil {
		return
	}
	pointer, err := NewTuplePointerFromBytes(buffer)
	if err != nil {
		return
	}
	// reset the pointer so that the pointed tuple will be considered "deleted"
	pointer.attrs.used = false
	// write back
	err = p.writeAt(pointer.toBytes(), tpStart)
	if err != nil {
		err = fmt.Errorf("failed to write tuple pointer back to the data: %w", err)
		return
	}
	// flush
	err = p.data.Sync()
	if err != nil {
		return
	}
	return
}

// ReadAll reads all tuples from a page
func (p *Page) ReadAll() (tuples []Tuple, err error) {
	if !p.initialized {
		err = p.Init()
		if err != nil {
			return
		}
	}
	err = p.readHeader()
	if err != nil {
		return
	}

	// read tuple pointers
	pointerCount, err := p.countTuplePointers()
	if err != nil {
		err = fmt.Errorf("failed to count tuple pointers: %w", err)
		return
	}
	buffer := make([]byte, tuplePointerSize*pointerCount)
	err = p.readAt(buffer, pageHeaderSize)
	if err != nil {
		err = fmt.Errorf("faild to read tuple pointers: %w", err)
		return
	}

	// we got the pointers
	pointers := make([]TuplePointer, pointerCount)
	var i uint32
	for i = 0; i < pointerCount; i++ {
		pointers[i], err = NewTuplePointerFromBytes(buffer[i*tuplePointerSize : (i+1)*tuplePointerSize])
		if err != nil {
			return
		}
	}

	// read tuples
	for idx, pointer := range pointers {
		// not used, skip
		if !pointer.attrs.used {
			continue
		}
		var tupleSize uint32
		// decide the tuple size
		if idx == 0 {
			// the first tuple (at the end of the data)
			tupleSize = uint32(pageSize - pointers[idx].dataPtr)
		} else {
			tupleSize = uint32(pointers[idx-1].dataPtr - pointers[idx].dataPtr)
		}
		buffer = make([]byte, tupleSize)
		err = p.readAt(buffer, uint32(pointer.dataPtr))
		if err != nil {
			err = fmt.Errorf("failed tp read tuple data: %w", err)
			return
		}
		tuples = append(tuples, buffer)
	}
	return
}

func (p *Page) countTuplePointers() (count uint32, err error) {
	pointerSectionSize := uint32(p.header.lower) - pageHeaderSize
	if pointerSectionSize%tuplePointerSize != 0 {
		err = fmt.Errorf("invalid pointer section size: %d", pointerSectionSize)
		return
	}
	count = pointerSectionSize / tuplePointerSize
	return
}

func (p *Page) readHeader() (err error) {
	buffer := make([]byte, pageHeaderSize)
	err = p.readAt(buffer, 0)
	if err != nil {
		if err == io.EOF {
			// nothing to read, using default config
			p.header = PageHeader{
				lower: PagePointer(pageHeaderSize),
				upper: pageSize,
			}
			err = nil
			return
		}
		return fmt.Errorf("failed to read page header: %w", err)
	}
	p.header.lower = PagePointer(endian.Uint32(buffer[pagePointerSize*0 : pagePointerSize*1]))
	p.header.upper = PagePointer(endian.Uint32(buffer[pagePointerSize*1 : pagePointerSize*2]))
	return
}

func (p *Page) writeAt(data []byte, position uint32) (err error) {
	written, err := p.data.WriteAt(data, int64(p.offset+uint64(position)))
	if err != nil {
		return
	} else if written != len(data) {
		err = errors.New("wrong number of bytes written into the page data")
		return
	}
	return
}

func (p *Page) readAt(data []byte, position uint32) (err error) {
	// read the content
	_, err = p.data.Seek(int64(p.offset+uint64(position)), io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek the page data: %w", err)
		return
	}
	read, err := p.data.Read(data)
	if err != nil {
		// for EOF, return as is since it might be normal sometimes
		if err == io.EOF {
			return err
		}
		err = fmt.Errorf("failed to read data from data: %w", err)
		return
	} else if read != len(data) {
		err = fmt.Errorf("mismatched number of bytes read")
		return
	}
	return
}

func uint32ToBytes(value uint32) []byte {
	buffer := make([]byte, unsafe.Sizeof(value))
	endian.PutUint32(buffer, value)
	return buffer
}

func bytesToUint32(data []byte) uint32 {
	return endian.Uint32(data)
}
