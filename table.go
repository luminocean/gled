package gled

import (
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/luminocean/gled/table"
	"github.com/vmihailenco/msgpack/v5"
	"regexp"
)

const (
	// permission used to create new db files
	filePerm = 0600
)

var (
	tableNameRegex = regexp.MustCompile(`^[0-9a-zA-Z_-]{1,32}$`)
)

type GledTable[T any] struct {
	table *table.Table
}

func (t *GledTable[T]) Insert(item T) (err error) {
	data, err := msgpack.Marshal(item)
	if err != nil {
		err = fmt.Errorf("failed to marshal item into JSON: %w", err)
		return
	}
	err = t.table.Add(data)
	if err != nil {
		err = fmt.Errorf("failed to insert item: %w", err)
		return
	}
	return
}

func (t *GledTable[T]) Select(ex exp.Ex) (items []T, locations []table.TupleLocation, err error) {
	err = t.table.Scan(func(tuple table.Tuple, loc table.TupleLocation) (cont bool, err error) {
		var unmarshalled map[string]any
		err = msgpack.Unmarshal(tuple, &unmarshalled)
		if err != nil {
			return
		}
		pass := exp.Eval(unmarshalled, ex)
		if pass {
			var item T
			err = msgpack.Unmarshal(tuple, &item)
			if err != nil {
				return
			}
			items = append(items, item)
			locations = append(locations, loc)
		}
		cont = true
		return
	})
	return
}

func (t *GledTable[T]) Delete(loc table.TupleLocation) (err error) {
	err = t.table.Delete(loc)
	if err != nil {
		return
	}
	return
}

func (t *GledTable[T]) Close() (err error) {
	err = t.table.Close()
	if err != nil {
		return
	}
	return
}
