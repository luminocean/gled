# GLED

Lightweight Embedded Database in Golang, inspired by SQLite and PostgreSQL.

## Usage

```go
package main

import (
	"fmt"
	"github.com/luminocean/gled"
	"github.com/luminocean/gled/exp"
)

type Book struct {
	Name  string
	Count int
}

func main() {
	// table files will be created under "."
	db := gled.NewGleDB(".")

	// create a new table for books
	table, _ := gled.Table[Book](db, "basic")
	defer table.Close()

	// insert one
	_ = table.Insert(Book{
		Name:  "mybook",
		Count: 10,
	})

	// select
	books, _, _ := table.Select(exp.AndEx{
		Exps: []exp.Ex{
			exp.C("Name").Eq("mybook"),
			exp.C("Count").Gte(5),
		},
	})

	// gives "[{mybook 10}]"
	fmt.Println(books)
}
```

## Roadmap

- [x] Multi-page support for Gled tables (currently only one page per table)
- [x] Multi-table/multi-database support
- [ ] Indexing
- [ ] SQL interface
- [ ] DB Vacuum

