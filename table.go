package gled

import "os"

// Table is a data structure to store data with the same schema
// which contains multiple pages
type Table struct {
	file *os.File
}

func NewTable(file *os.File) *Table {
	return &Table{
		file: file,
	}
}

func (t *Table) Add(tuple Tuple) (err error) {
	return
}
