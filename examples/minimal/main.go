package main

import (
	"github.com/luminocean/gled"
	"os"
	"strings"
)

type Book struct {
	Name string
}

func main() {
	// file to store data
	file, _ := os.OpenFile("./data.gled", os.O_CREATE|os.O_RDWR, 0600)
	defer file.Close()

	// initialize a db instance
	// (think of a table in other databases)
	ins := gled.NewInstance[Book](file)
	ins.Initialize()

	// insert data
	book := Book{
		Name: "mybook",
	}
	ins.Insert(book)

	// select data
	books, _ := ins.Select(func(b Book) bool {
		// select books whose name starts with "my"
		return strings.HasPrefix(b.Name, "my")
	})
	if books[0].Name != book.Name {
		panic(nil)
	}

	// delete data
	ins.Delete(func(b Book) bool {
		// delete all
		return true
	})

	// select again
	books, _ = ins.Select(func(b Book) bool {
		return true
	})
	if len(books) != 0 {
		panic(nil)
	}
}
