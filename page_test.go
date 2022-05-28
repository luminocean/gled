package gled

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestFlushAndLoad(t *testing.T) {
	items := []string{
		"this is some data",
		"have a nice day",
		"good bye",
	}

	page1 := NewPage()
	for _, item := range items {
		err := page1.Add(Tuple(item))
		assert.NoError(t, err)
	}
	file, err := ioutil.TempFile("", "gled_ut_*")
	assert.NoError(t, err)
	defer os.Remove(file.Name())
	defer file.Close()

	err = page1.Flush(file)
	assert.NoError(t, err)

	page2 := NewPage()
	err = page2.Load(file)
	assert.NoError(t, err)

	assert.EqualValues(t, page1, page2)
}
