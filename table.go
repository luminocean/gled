package gled

import (
	"fmt"
	"io"
	"os"
)

// Table is a data structure to store data with the same schema
// which contains multiple pages
type Table struct {
	// data file
	data *os.File
	// free space map file
	fsm *os.File
}

func NewTable(data *os.File, fsm *os.File) *Table {
	return &Table{
		data: data,
		fsm:  fsm,
	}
}

func (t *Table) Add(tuple Tuple) (err error) {
	idx, err := t.getFreePageIndex(uint32(len(tuple)))
	if err != nil {
		return
	}
	if idx == -1 {
		// no free page found, create a new one
		idx, err = t.allocateNewPage()
		if err != nil {
			return
		}
	}
	// TODO: found the page index, load the page
	return
}

// find the page index that can hold a tuple with size |minSize|
func (t *Table) getFreePageIndex(minSize uint32) (idx int64, err error) {
	chunkSize := 1024
	totalBytesRead := 0
	buff := make([]byte, chunkSize)
	for i := 0; ; i++ {
		// how many bytes read
		var read int
		read, err = t.fsm.Read(buff)
		if err != nil {
			if err == io.EOF {
				// all bytes read. done reading
				err = nil
				break
			}
			return
		}
		totalBytesRead += read
		// find a free page from the bytes read from FSM
		// this is a simplified version of
		// https://github.com/postgres/postgres/blob/7db0cde6b58eef2ba0c70437324cbc7622230320/src/backend/storage/freespace/README
		for j, b := range buff {
			// 0 - 255
			capacity := uint32(b)
			// how many bytes 1 unit in the capacity above stands for
			density := uint32(pageSize / 256)
			freeSpace := pageSize - capacity*density
			if freeSpace >= minSize {
				// found the page index
				idx = int64(i*chunkSize + j)
				return
			}
		}
		// all bytes read. done reading
		if read < chunkSize {
			break
		}
	}
	idx = -1
	return
}

func (t *Table) allocateNewPage() (idx int64, err error) {
	// seek to the end of the FSM file
	offset, err := t.fsm.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}
	_, err = t.fsm.Write([]byte{0})
	if err != nil {
		err = fmt.Errorf("failed to write new FSM byte: %w", err)
		return
	}
	idx = offset + 1
	return
}
