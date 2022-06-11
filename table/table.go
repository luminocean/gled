package table

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
)

const (
	// how many bytes 1 unit in the Fsm capacity value (as a byte) stands for
	fsmDensity = PageSize / 256
)

// Iterator is a callback for each tuple in a table
type Iterator func(tuple Tuple, loc TupleLocation) (cont bool, err error)

// Table is a data structure to store data with the same schema
// which contains multiple pages
type Table struct {
	// data file
	file *os.File
	// free space map file
	fsm *os.File
}

func NewTable(data *os.File, fsm *os.File) *Table {
	return &Table{
		file: data,
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
	p := NewPage(t.file, uint64(idx*PageSize))
	if err != nil {
		return
	}
	_, free, err := p.Add(tuple)
	if err != nil {
		return
	}

	err = t.updateFsm(idx, free)
	if err != nil {
		return
	}
	return
}

func (t *Table) Scan(iter Iterator) (err error) {
	info, err := t.file.Stat()
	if err != nil {
		return
	}
	size := info.Size()
	if size%PageSize != 0 {
		log.Warn().Msgf("size of file %s %d is not a multiple of the page size %d", t.file.Name(), size, PageSize)
	}
	pageCount := size / PageSize
	for i := int64(0); i < pageCount; i++ {
		p := NewPage(t.file, uint64(i*PageSize))
		var tps []Tuple
		tps, err = p.ReadAll()
		if err != nil {
			return err
		}
		for j, tp := range tps {
			var cont bool
			cont, err = iter(tp, TupleLocation{
				Page:   i,
				Offset: uint32(j),
			})
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

func (t *Table) Delete(loc TupleLocation) (err error) {
	p := NewPage(t.file, uint64(loc.Page*PageSize))
	if err != nil {
		return
	}
	err = p.Remove(loc.Offset)
	if err != nil {
		return
	}
	return
}

func (t *Table) Flush() (err error) {
	err = t.file.Sync()
	if err != nil {
		err = fmt.Errorf("failed to flush table file file: %w", err)
		return
	}
	err = t.fsm.Sync()
	if err != nil {
		err = fmt.Errorf("failed to flush table fsm file: %w", err)
		return
	}
	return
}

func (t *Table) Close() (err error) {
	err = t.Flush()
	if err != nil {
		return
	}
	errMsg := ""
	dataCloseErr := t.file.Close()
	if dataCloseErr != nil {
		errMsg += fmt.Sprintf("failed to close data file %s", t.fsm.Name())
	}
	fsmCloseErr := t.fsm.Close()
	if fsmCloseErr != nil {
		if errMsg != "" {
			errMsg += "; "
		}
		errMsg += fmt.Sprintf("failed to close fsm file %s", t.fsm.Name())
	}
	if errMsg != "" {
		err = errors.New(errMsg)
		return
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
		for j, b := range buff[:read] {
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
	idx = offset
	return
}

// update the remaining free space for a page in the Fsm file
func (t *Table) updateFsm(idx int64, freeSpace uint32) (err error) {
	capacity := fsmFreeSpaceToCapacity(freeSpace)
	_, err = t.fsm.WriteAt([]byte{capacity}, idx)
	if err != nil {
		return
	}
	return
}

func fsmCapacityToFreeSpace(capacity byte) (freeSpace uint32) {
	// 0 - 255
	c := uint32(capacity)
	// 0 - 8192
	freeSpace = PageSize - (c+1)*fsmDensity
	return
}

func fsmFreeSpaceToCapacity(freeSpace uint32) (capacity byte) {
	// 0 - 8192
	used := PageSize - freeSpace
	// 0 - 256
	ratio := used / fsmDensity
	if ratio == 256 {
		// prevent overflow
		ratio -= 1
	}
	capacity = byte(ratio)
	return
}
