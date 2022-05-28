package gled

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// The page layout is borrowed from https://www.interdb.jp/pg/pgsql01.html

const (
	PageSize        = 1024 * 8
	PageHeaderSize  = 8
	PagePointerSize = 4 // size of int32
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

type PageOutput interface {
	io.Writer
	io.Seeker
}

type PageInput interface {
	io.Reader
	io.Seeker
}

type PagePointer uint32

type Page struct {
	header   PageHeader
	pointers []PagePointer
	tuples   []Tuple
}

func NewPage() *Page {
	return &Page{
		header: PageHeader{
			lower: PageHeaderSize,
			upper: PageSize,
		},
	}
}

func (p *Page) Add(tuple Tuple) (err error) {
	if p.header.lower+PagePointerSize+tuple.Size() >= p.header.upper {
		err = errors.New("no room for more tuples")
		return
	}
	// add the tuple
	p.tuples = append(p.tuples, tuple)
	p.header.upper -= tuple.Size()

	// add a pointer for the new tuple
	p.pointers = append(p.pointers, PagePointer(p.header.upper))
	p.header.lower += PagePointerSize
	return
}

func (p *Page) Flush(out PageOutput) (err error) {
	// ensure we start from the beginning
	_, err = out.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	endian := binary.BigEndian
	// write header
	buffer := make([]byte, 8)
	endian.PutUint32(buffer[0:4], p.header.lower)
	endian.PutUint32(buffer[4:8], p.header.upper)
	_, err = out.Write(buffer)
	if err != nil {
		return
	}
	// write pointers
	buffer = make([]byte, len(p.pointers)*PagePointerSize)
	for idx, pointer := range p.pointers {
		endian.PutUint32(buffer[idx*4:(idx+1)*4], uint32(pointer))
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
	for i := len(p.tuples) - 1; i >= 0; i-- {
		tuple := p.tuples[i]
		_, err = out.Write(tuple)
		if err != nil {
			return
		}
	}
	return
}

func (p *Page) Load(in PageInput) (err error) { // ensure we start from the beginning
	_, err = in.Seek(0, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek the page file: %w", err)
		return
	}
	// read page header
	buffer := make([]byte, PageHeaderSize)
	read, err := in.Read(buffer)
	if err != nil {
		err = fmt.Errorf("failed to read page header: %w", err)
		return
	} else if read != PageHeaderSize {
		err = fmt.Errorf("mismatched bytes read for page header")
		return
	}
	endian := binary.BigEndian
	p.header.lower = endian.Uint32(buffer[:4])
	p.header.upper = endian.Uint32(buffer[4:8])

	// read pointers
	pointerSectionSize := p.header.lower - PageHeaderSize
	if pointerSectionSize%PagePointerSize != 0 {
		err = fmt.Errorf("invalid pointer section size: %d", pointerSectionSize)
		return
	}
	pointerCount := pointerSectionSize / PagePointerSize
	buffer = make([]byte, PagePointerSize*pointerCount)
	read, err = in.Read(buffer)
	if err != nil {
		err = fmt.Errorf("failed to read page pointers: %w", err)
		return
	} else if uint32(read) != PagePointerSize*pointerCount {
		err = fmt.Errorf("mismatched bytes read for page pointers")
		return
	}
	p.pointers = make([]PagePointer, pointerCount)
	var i uint32
	for i = 0; i < pointerCount; i++ {
		p.pointers[i] = PagePointer(endian.Uint32(buffer[PagePointerSize*i : PagePointerSize*(i+1)]))
	}

	// read tuples
	tupleCount := pointerCount
	p.tuples = make([]Tuple, tupleCount)
	for idx, pointer := range p.pointers {
		var tupleSize uint32
		// decide the tuple size
		if idx == 0 {
			// the first tuple (at the end of the file)
			tupleSize = uint32(PageSize - p.pointers[idx])
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
