package table

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestPageBasic(t *testing.T) {
	inputTuples := []Tuple{
		Tuple("here's some file"),
		Tuple("have a nice day"),
		Tuple("good bye"),
	}

	file, err := ioutil.TempFile("", "gled_ut_*")
	assert.NoError(t, err)
	defer os.Remove(file.Name())
	defer file.Close()

	// write tuples in page1
	page1 := NewPage(file, 0)
	defer page1.Close()
	for _, item := range inputTuples {
		_, _, err := page1.Add(item)
		assert.NoError(t, err)
	}
	// remove one tuple
	err = page1.Remove(1)
	assert.NoError(t, err)

	// read the content through another page
	page2 := NewPage(file, 0)
	defer page2.Close()
	outputTuples, err := page2.ReadAll()
	assert.NoError(t, err)

	// check if the tuples are expected after deletion
	assert.EqualValues(t, []Tuple{inputTuples[0], inputTuples[2]}, outputTuples)

	// add a new tuple, which should reuse an existing index
	idx, _, err := page2.Add(inputTuples[1])
	outputTuples, err = page2.ReadAll()
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), idx)
	assert.EqualValues(t, inputTuples, outputTuples)
}
