package gled

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestPageWriteAndRead(t *testing.T) {
	inputTuples := []Tuple{
		Tuple("here's some data"),
		Tuple("have a nice day"),
		Tuple("good bye"),
	}

	file, err := ioutil.TempFile("", "gled_ut_*")
	assert.NoError(t, err)
	defer os.Remove(file.Name())
	defer file.Close()

	page1 := NewPage(file, 0)
	for _, item := range inputTuples {
		_, err := page1.Add(item)
		assert.NoError(t, err)
	}

	err = page1.Remove(1)
	assert.NoError(t, err)

	page2 := NewPage(file, 0)
	outputTuples, err := page2.ReadAll()
	assert.NoError(t, err)

	assert.EqualValues(t, []Tuple{inputTuples[0], inputTuples[2]}, outputTuples)
}
