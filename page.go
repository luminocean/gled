package gled

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

// The page layout is borrowed from https://www.interdb.jp/pg/pgsql01.html

const (
	// size of one page
	pageSize = 1024 * 8
	// size of uint32
	pagePointerSize = 4
)

var (
	// pageHeaderSize is the size of a page header
	pageHeaderSize = uint32(unsafe.Sizeof(PageHeader{}))
	// endian for all data bytes in a page
	endian = binary.BigEndian
)

type PageHeader struct {
	// Lower is the starting position of the free space (inclusive)
	lower uint32
	// Lower is the ending position of the free space (exclusive)
	upper uint32
}

type Tuple []byte

func (t *Tuple) Size() uint32 {
	return uint32(len(*t))
}

type PagePointer uint32

type Page struct {
	header   PageHeader
	pointers []PagePointer
	tuples   []Tuple
}

// NewPage creates and initializes a new page
func NewPage() *Page {
	return &Page{
		header: PageHeader{
			lower: pageHeaderSize,
			upper: pageSize,
		},
	}
}

// Add adds a tuple to the page
func (p *Page) Add(tuple Tuple) (err error) {
	if p.header.lower+pagePointerSize+tuple.Size() >= p.header.upper {
		err = errors.New("no room for more tuples")
		return
	}
	// add the tuple
	p.tuples = append(p.tuples, tuple)
	p.header.upper -= tuple.Size()

	// add a pointer for the new tuple
	p.pointers = append(p.pointers, PagePointer(p.header.upper))
	p.header.lower += pagePointerSize
	return
}

// All lists all the tuples in the page
func (p *Page) All() (tuples []Tuple) {
	return p.tuples
}

// Flush writes data on the page to the page file
func (p *Page) Flush(out DataWritable) (err error) {
	// ensure we start from the beginning
	_, err = out.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	// write header
	buffer := make([]byte, pageHeaderSize)
	endian.PutUint32(buffer[0:4], p.header.lower)
	endian.PutUint32(buffer[4:8], p.header.upper)
	_, err = out.Write(buffer)
	if err != nil {
		return
	}

	// write pointers
	buffer = make([]byte, len(p.pointers)*pagePointerSize)
	for idx, pointer := range p.pointers {
		endian.PutUint32(buffer[idx*pagePointerSize:(idx+1)*pagePointerSize], uint32(pointer))
	}
	_, err = out.Write(buffer)
	if err != nil {
		return
	}

	// write tuples
	_, err = out.Seek(int64(p.header.upper), io.SeekStart)
	if err != nil {
		return
	}
	// reversed order
	for i := len(p.tuples) - 1; i >= 0; i-- {
		tuple := p.tuples[i]
		_, err = out.Write(tuple)
		if err != nil {
			return
		}
	}
	return
}

func (p *Page) Load(in DataReadable) (err error) { // ensure we start from the beginning
	_, err = in.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek the page file: %w", err)
		return
	}
	// read page header
	buffer := make([]byte, pageHeaderSize)
	read, err := in.Read(buffer)
	if err != nil {
		if err == io.EOF {
			// if |in| is an empty, we shall stop and treat it as a regular yet empty source
			// and nothing else needs to be done
			err = nil
			return
		}
		err = fmt.Errorf("failed to read page header: %w", err)
		return
	} else if uint32(read) != pageHeaderSize {
		err = fmt.Errorf("mismatched bytes read for page header")
		return
	}
	p.header.lower = endian.Uint32(buffer[:4])
	p.header.upper = endian.Uint32(buffer[4:8])

	// read pointers
	pointerSectionSize := p.header.lower - pageHeaderSize
	if pointerSectionSize%pagePointerSize != 0 {
		err = fmt.Errorf("invalid pointer section size: %d", pointerSectionSize)
		return
	}
	pointerCount := pointerSectionSize / pagePointerSize
	buffer = make([]byte, pagePointerSize*pointerCount)
	read, err = in.Read(buffer)
	if err != nil {
		err = fmt.Errorf("failed to read page pointers: %w", err)
		return
	} else if uint32(read) != pagePointerSize*pointerCount {
		err = fmt.Errorf("mismatched bytes read for page pointers")
		return
	}
	p.pointers = make([]PagePointer, pointerCount)
	var i uint32
	for i = 0; i < pointerCount; i++ {
		p.pointers[i] = PagePointer(endian.Uint32(buffer[pagePointerSize*i : pagePointerSize*(i+1)]))
	}

	// read tuples
	tupleCount := pointerCount
	p.tuples = make([]Tuple, tupleCount)
	for idx, pointer := range p.pointers {
		var tupleSize uint32
		// decide the tuple size
		if idx == 0 {
			// the first tuple (at the end of the file)
			tupleSize = uint32(pageSize - p.pointers[idx])
		} else {
			tupleSize = uint32(p.pointers[idx-1] - p.pointers[idx])
		}
		buffer = make([]byte, tupleSize)
		// read the content
		_, err = in.Seek(int64(pointer), io.SeekStart)
		if err != nil {
			err = fmt.Errorf("failed to seek the page file: %w", err)
			return
		}
		read, err = in.Read(buffer)
		if err != nil {
			err = fmt.Errorf("failed to read page tuples: %w", err)
			return
		} else if uint32(read) != tupleSize {
			err = fmt.Errorf("mismatched bytes read for page tuples")
			return
		}
		p.tuples[idx] = buffer
	}
	return
}
