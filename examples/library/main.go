package main

/* Example of how to use Instance as a go library. */

import (
	"fmt"
	"github.com/luminocean/gled"
	"os"
	"path/filepath"
	"strings"
)

type Cell struct {
	Key   string
	Value string
}

func main() {
	filePath, err := filepath.Abs("./data.gled")
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, gled.DataFilePerms)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	instance := gled.NewInstance[Cell](file)
	err = instance.Init()
	if err != nil {
		panic(err)
	}

	err = instance.Insert(Cell{
		Key:   "hello",
		Value: "world",
	})
	if err != nil {
		panic(err)
	}

	results, err := instance.Select(func(cell Cell) bool {
		return strings.HasPrefix(cell.Value, "wor")
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(results)
}
