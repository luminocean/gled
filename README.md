# GLED: Lightweight Embedded Database in Golang

Gled is an embedded relational database implemented in pure Go, inspired by SQLite and PostgreSQL.

## Usage

**Currently Gled only supports one table with a single schema.** You can use any struct as the table schema (as long as it's JSON serializable):

```go
type Book struct {
	Name string
}
```

Then you can initialize your database with it:

```go
// create and open the db file
file, _ := os.OpenFile("./data.gled", os.O_CREATE|os.O_RDWR, 0600)
// initialize the instance
ins := gled.NewInstance[Book](file)
ins.Initialize()
```

Now you can go head and insert data:

```go
ins.Insert(Book{
	  Name: "mybook",
})
```

Gled accepts a callback function to run select queries which is way more powerful flexible than plain SQL:

```go
books, _ := ins.Select(func(b Book) bool {
		// select books whose name starts with "my"
		return strings.HasPrefix(b.Name, "my")
})
```

Of course you can delete data using the same approach:

```go
ins.Delete(func(b Book) bool {
		// delete all
		return true
})
```

A full example:
```go
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
```

## Roadmap

- Multi-page support for Gled tables (currently only one page per table)
- Multi-database support
- DB Vacuum
- Indexing
