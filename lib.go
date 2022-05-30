package gled

import (
	"encoding/json"
	"fmt"
	"os"
)

type Instance[T any] struct {
	page *Page
}

func NewInstance[T any](dataFile *os.File) *Instance[T] {
	return &Instance[T]{
		page: NewPage(dataFile, 0),
	}
}

func (ins *Instance[T]) Initialize() (err error) {
	err = ins.page.Init()
	if err != nil {
		return
	}
	return
}

func (ins *Instance[T]) Insert(data T) (err error) {
	bs, err := json.Marshal(data)
	if err != nil {
		return
	}
	err = ins.page.Add(bs)
	if err != nil {
		return
	}
	return
}

func (ins *Instance[T]) Select(selector func(T) bool) (results []T, err error) {
	tuples, _, err := ins.page.ReadAll()
	if err != nil {
		return
	}
	for _, tuple := range tuples {
		var item T
		err = json.Unmarshal(tuple, &item)
		if err != nil {
			err = fmt.Errorf("failed to unmarshal tuple data into %T: %w", item, err)
			return
		}
		if selector(item) {
			results = append(results, item)
		}
	}
	return
}

func (ins *Instance[T]) Delete(selector func(T) bool) (deleted int, err error) {
	tuples, tpIdxes, err := ins.page.ReadAll()
	if err != nil {
		return
	}
	for idx, tuple := range tuples {
		var item T
		err = json.Unmarshal(tuple, &item)
		if err != nil {
			err = fmt.Errorf("failed to unmarshal tuple data into %T: %w", item, err)
			return
		}
		if selector(item) {
			err = ins.page.Remove(tpIdxes[idx])
			if err != nil {
				err = fmt.Errorf("failed to delete tuple: %w", err)
				return
			}
		}
	}
	return
}
