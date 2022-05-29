package main

/* Example of how to use Instance as a go library. */

import (
	"fmt"
	"github.com/luminocean/gled"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Cell struct {
	Key   string
	Value int
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
	err = instance.Initialize()
	if err != nil {
		panic(err)
	}

	// insert an odd and an even number
	rand.Seed(time.Now().Unix())
	r := rand.Intn(1000)
	for i := 0; i < 2; i++ {
		err = instance.Insert(Cell{
			Key:   "hello world",
			Value: r + i,
		})
		if err != nil {
			panic(err)
		}
	}

	// select all
	results, err := instance.Select(func(cell Cell) bool {
		return strings.HasPrefix(cell.Key, "hello")
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("all: %v\n", results)

	// delete cells with odd value and select again
	_, err = instance.Delete(func(cell Cell) bool {
		return cell.Value%2 == 1
	})
	if err != nil {
		panic(err)
	}
	results, err = instance.Select(func(cell Cell) bool {
		return strings.HasPrefix(cell.Key, "hello")
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("now: %v\n", results)
}
