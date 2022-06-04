package gled

import (
	"fmt"
	"github.com/luminocean/gled/storage"
	"os"
	"path"
)

type GledDB struct {
	dir string
}

func NewGleDB(directory string) *GledDB {
	return &GledDB{dir: directory}
}

func Table[T any](db *GledDB, name string) (table *GledTable[T], err error) {
	dirInfo, err := os.Stat(db.dir)
	if err != nil {
		err = fmt.Errorf("failed to check db directory: %w", err)
		return
	}
	if !dirInfo.IsDir() {
		err = fmt.Errorf("%s is not a directory", db.dir)
		return
	}
	if !tableNameRegex.MatchString(name) {
		err = fmt.Errorf("invalid db name: %s", name)
		return
	}
	dataPath := path.Join(db.dir, fmt.Sprintf("%s.gled", name))
	fsmPath := path.Join(db.dir, fmt.Sprintf("%s.fsm.gled", name))

	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		err = fmt.Errorf("failed to open data file %s: %w", dataPath, err)
		return
	}
	fsmFile, err := os.OpenFile(fsmPath, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		err = fmt.Errorf("failed to open fsm file %s: %w", fsmPath, err)
		return
	}
	table = &GledTable[T]{
		table: storage.NewTable(dataFile, fsmFile),
	}
	return
}
