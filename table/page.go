package table

import (
	"errors"
	"fmt"
	"github.com/luminocean/gled/util"
	"io"
	"os"
)

// The page layout is borrowed from https://www.interdb.jp/pg/pgsql01.html

const (
	// PageSize is the size of one page
	PageSize = 1024 * 8
)

// PagePointer is a pointer pointing to a location within a page
type PagePointer uint32

// Page is a fixed-length area in a file to store tuples and related data structures
type Page struct {
	file *os.File
	// where the page starts at the data file
	offset      uint64
	header      PageHeader
	initialized bool
}

// NewPage creates and initializes a new page
// from a specific offset of a file
func NewPage(data *os.File, offset uint64) *Page {
	return &Page{
		header:      PageHeader{},
		file:        data,
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
// returns the remaining free spaces
func (p *Page) Add(tuple Tuple) (idx uint32, free uint32, err error) {
	if !p.initialized {
		err = p.Init()
		if err != nil {
			return
		}
	}

	// pointer data for the new tuple
	var tuplePointer TuplePointer
	var tuplePointerOffset uint32 = 0

	// go through existing tuple pointers to see if we can reuse previously deleted ones
	tpSectionSize := uint32(p.header.lower) - pageHeaderSize
	pointerCount := tpSectionSize / tuplePointerSize
	// assuming the new tuple index is a new one, unless we find a reusable one later
	idx = pointerCount

	buffer := make([]byte, tpSectionSize)
	err = p.readAt(buffer, pageHeaderSize)
	if err != nil {
		return
	}
	var i uint32
	for i = 0; i < pointerCount; i++ {
		var p TuplePointer
		p, err = NewTuplePointerFromBytes(buffer[tuplePointerSize*i : tuplePointerSize*(i+1)])
		if err != nil {
			return
		}
		if !p.attrs.used {
			tuplePointer = p
			idx = i
			tuplePointerOffset = pageHeaderSize + tuplePointerSize*idx
			break
		}
	}

	// the new offset in the page after the tuple insertion
	newExpectedOffset := uint32(p.header.lower) + tuple.Size()
	if tuplePointerOffset == 0 {
		// no existing reusable tuple pointer, we need to create a new one
		newExpectedOffset += tuplePointerSize + tuple.Size()
	}
	// check if we have enough room to accomodate the new tuple
	if newExpectedOffset > uint32(p.header.upper) {
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
	// no existing pointer, new one
	if tuplePointerOffset == 0 {
		tuplePointerOffset = uint32(p.header.lower)
		tuplePointer = TuplePointer{
			attrs: tupleAttributes{
				used: true,
			},
			dataPtr:  PagePointer(tupleStart),
			dataSize: uint32(len(tuple)),
		}
		// update lower as we are adding a new tuple pointer
		p.header.lower = PagePointer(uint32(p.header.lower) + tuplePointerSize)
	} else {
		// otherwise update the existing pointer
		tuplePointer.dataPtr = PagePointer(tupleStart)
		tuplePointer.dataSize = uint32(len(tuple))
		tuplePointer.attrs.used = true
	}
	// write the tuple pointer
	err = p.writeAt(tuplePointer.toBytes(), tuplePointerOffset)
	if err != nil {
		return
	}

	// write updated lower and upper pointers
	err = p.writeAt(p.header.toBytes(), 0)
	if err != nil {
		return
	}

	fs := p.freeSize()
	if fs < 0 {
		panic("negative free size after adding a new tuple")
	}
	free = uint32(fs)
	return
}

// Remove removes a tuple by providing the pointer index (starting from 0) pointing to the tuple
// Note that FSM is not updated until we do vacuuming
func (p *Page) Remove(idx uint32) (err error) {
	if !p.initialized {
		err = p.Init()
		if err != nil {
			return
		}
	}

	pointerCount, err := p.countTuplePointers()
	if err != nil {
		err = fmt.Errorf("failed to count tuple pointers: %w", err)
		return
	}
	if idx >= pointerCount {
		err = fmt.Errorf("tuple pointer index too large")
		return
	}
	// read the pointer of the tuple to be deleted by index
	tpStart := pageHeaderSize + tuplePointerSize*idx
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
		err = fmt.Errorf("failed to write tuple pointer back to the file: %w", err)
		return
	}
	return
}

func (p *Page) Flush() (err error) {
	err = p.file.Sync()
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
	for _, pointer := range pointers {
		// not used, skip
		if !pointer.attrs.used {
			continue
		}
		buffer = make([]byte, pointer.dataSize)
		err = p.readAt(buffer, uint32(pointer.dataPtr))
		if err != nil {
			err = fmt.Errorf("failed tp read tuple file: %w", err)
			return
		}
		tuples = append(tuples, buffer)
	}
	return
}

func (p *Page) Close() (err error) {
	if !p.initialized {
		err = errors.New("page already closed")
		return
	}
	err = p.Flush()
	if err != nil {
		return
	}
	// if there's an error during Flush, the page is not considered as closed
	p.initialized = false
	return
}

// returns (the remaining hole size - one tuple pointer)
func (p *Page) freeSize() int32 {
	return int32(p.header.upper) - int32(p.header.lower) - int32(tuplePointerSize)
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
				upper: PageSize,
			}
			err = nil
			return
		}
		return fmt.Errorf("failed to read page header: %w", err)
	}
	err = p.header.fromBytes(buffer)
	if err != nil {
		return
	}
	return
}

func (p *Page) writeAt(data []byte, position uint32) (err error) {
	err = util.WriteAt(p.file, data, int64(p.offset+uint64(position)))
	if err != nil {
		return
	}
	return
}

func (p *Page) readAt(buffer []byte, position uint32) (err error) {
	err = util.ReadAt(p.file, buffer, int64(p.offset+uint64(position)))
	if err != nil {
		return
	}
	return
}
