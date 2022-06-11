package table

import (
	"bufio"
	"bytes"
	"fmt"
	"unsafe"
)

var (
	// size of a tuple pointer
	tuplePointerSize = uint32(unsafe.Sizeof(TuplePointer{}))
)

// TuplePointer points to a tuple within a page
type TuplePointer struct {
	attrs    tupleAttributes
	dataPtr  PagePointer
	dataSize uint32
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
	pointer.dataPtr = PagePointer(bytesToUint32(data[1:5]))
	pointer.dataSize = bytesToUint32(data[5:9])
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
	_, err = w.Write(uint32ToBytes(p.dataSize))
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

// postgres equivalent:
// https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/storage/itemid.h#L38
type tupleAttributes struct {
	// whether the pointer is in use
	used bool
}

// TupleLocation is the location where a tuple is on a page
type TupleLocation struct {
	Page   int64
	Offset uint32
}
