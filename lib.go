package gled

import (
	"encoding/json"
)

type Instance[T any] struct {
	file DataFile
	page *Page
}

func NewInstance[T any](dataFile DataFile) *Instance[T] {
	return &Instance[T]{
		file: dataFile,
		page: NewPage(),
	}
}

func (ins *Instance[T]) Init() (err error) {
	err = ins.page.Load(ins.file)
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
	// flush immediately to make the API simple, but obviously with less performance
	err = ins.page.Flush(ins.file)
	return
}

func (ins *Instance[T]) Select(selector func(T) bool) (results []T, err error) {
	tuples := ins.page.All()
	for _, tuple := range tuples {
		var item T
		err = json.Unmarshal(tuple, &item)
		if err != nil {
			return
		}
		if selector(item) {
			results = append(results, item)
		}
	}
	return
}
