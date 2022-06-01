package gled

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
)

const (
	// how many bytes 1 unit in the fsm capacity value (as a byte) stands for
	fsmDensity = pageSize / 256
)

type TableIterator func(tuple Tuple, page int64, offset int64) (cont bool, err error)

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

// Add adds a tuple to a table
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

	// open the underlying page and add
	page := NewPage(t.data, uint64(idx*pageSize))
	if err != nil {
		return
	}
	free, err := page.Add(tuple)
	if err != nil {
		return
	}

	err = t.updateFsm(idx, free)
	if err != nil {
		return
	}
	return
}

func (t *Table) Scan(iter TableIterator) (err error) {
	info, err := t.data.Stat()
	if err != nil {
		return
	}
	size := info.Size()
	if size%pageSize != 0 {
		log.Warn().Msgf("size of file %s %d is not a multiple of the page size %d", t.data.Name(), size, pageSize)
	}
	pageCount := size / pageSize
	for i := int64(0); i < pageCount; i++ {
		page := NewPage(t.data, uint64(i*pageSize))
		var tps []Tuple
		tps, err = page.ReadAll()
		if err != nil {
			return err
		}
		for j, tp := range tps {
			var cont bool
			cont, err = iter(tp, i, int64(j))
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
	}
	return
}

// find the page index that can hold a tuple with size |minSize|
func (t *Table) getFreePageIndex(minSize uint32) (idx int64, err error) {
	chunkSize := 1024
	buff := make([]byte, chunkSize)
	for i := 0; ; i++ {
		_, err = t.fsm.Seek(0, io.SeekStart)
		if err != nil {
			return
		}
		var read int
		read, err = t.fsm.Read(buff)
		if read == 0 {
			return -1, nil
		}
		// find a free page from the bytes read from FSM
		// this is a simplified version of
		// https://github.com/postgres/postgres/blob/7db0cde6b58eef2ba0c70437324cbc7622230320/src/backend/storage/freespace/README
		for j, b := range buff {
			freeSpace := fsmCapacityToFreeSpace(b)
			if freeSpace >= minSize {
				// found the index of the page that has enough room for the new tuple
				idx = int64(i*chunkSize + j)
				return
			}
		}
		// err check is placed after is because it's possible
		// that Read() returns both a non-zero read and non-nil err
		if err != nil {
			if err == io.EOF {
				// all bytes read. done reading
				err = nil
				break
			}
			return
		}
		// we've read all data, nothing to do
		if read < chunkSize {
			break
		}
	}
	return -1, nil
}

// allocate a new page at the end of the table file, so that we can hold more data
// returns the new page index
func (t *Table) allocateNewPage() (idx int64, err error) {
	// seek to the end of the FSM file
	// and add a new byte indicating a new page
	offset, err := t.fsm.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}
	_, err = t.fsm.Write([]byte{0})
	if err != nil {
		err = fmt.Errorf("failed to write new FSM byte: %w", err)
		return
	}
	err = t.fsm.Sync()
	if err != nil {
		return
	}
	idx = offset
	return
}

func (t *Table) updateFsm(idx int64, freeSpace uint32) (err error) {
	capacity := fsmFreeSpaceToCapacity(freeSpace)
	_, err = t.fsm.WriteAt([]byte{capacity}, idx)
	if err != nil {
		return
	}
	err = t.fsm.Sync()
	if err != nil {
		return
	}
	return
}

func fsmCapacityToFreeSpace(capacity byte) (freeSpace uint32) {
	// 0 - 255
	c := uint32(capacity)
	// 0 - 8192
	freeSpace = pageSize - (c+1)*fsmDensity
	return
}

func fsmFreeSpaceToCapacity(freeSpace uint32) (capacity byte) {
	// 0 - 8192
	used := pageSize - freeSpace
	// 0 - 255
	ratio := used / fsmDensity
	if ratio == 256 {
		// prevent overflow
		ratio -= 1
	}
	capacity = byte(ratio)
	return
}
