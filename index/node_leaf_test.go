package index

import (
	"github.com/luminocean/gled/exp"
	"io/ioutil"
	"os"
	"testing"
)

func TestLeafNode(t *testing.T) {
	f, err := ioutil.TempFile("", "gled_ut_ln_data_")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	node, err := AllocateNewLeafNode(f)
	if err != nil {
		panic(err)
	}
	err = node.Insert(exp.String("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}
	err = node.Flush()
	if err != nil {
		panic(err)
	}

	other := NewLeafNode(f, 0)
	err = other.Initialize()
	if err != nil {
		panic(err)
	}
}
