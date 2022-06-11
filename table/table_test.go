package table

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestTableCRD(t *testing.T) {
	inputTuples := []Tuple{
		Tuple("here's some file"),
		Tuple("have a nice day"),
		Tuple("good bye"),
	}

	data, err := ioutil.TempFile("", "gled_ut_tbl_data_*")
	assert.NoError(t, err)
	defer os.Remove(data.Name())
	defer data.Close()

	fsm, err := ioutil.TempFile("", "gled_ut_tbl_fsm_*")
	assert.NoError(t, err)
	defer os.Remove(fsm.Name())
	defer fsm.Close()

	// create
	table := NewTable(data, fsm)
	defer table.Close()
	for _, item := range inputTuples {
		err := table.Add(item)
		assert.NoError(t, err)
	}

	// retrieve
	outputTuples := []Tuple{}
	locations := []TupleLocation{}
	err = table.Scan(func(t Tuple, loc TupleLocation) (bool, error) {
		outputTuples = append(outputTuples, t)
		locations = append(locations, loc)
		return true, nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, inputTuples, outputTuples)

	// delete the middle one
	err = table.Delete(locations[1])
	assert.NoError(t, err)

	// retrieve again
	outputTuples = []Tuple{}
	err = table.Scan(func(t Tuple, loc TupleLocation) (bool, error) {
		outputTuples = append(outputTuples, t)
		locations = append(locations, loc)
		return true, nil
	})
	assert.NoError(t, err)
	// no middle one
	assert.EqualValues(t, append(inputTuples[:1], inputTuples[2:]...), outputTuples)
}

func TestTableWriteAndReadBulk(t *testing.T) {
	inputTuples := []Tuple{}
	// 40 bytes per batch
	batch := []Tuple{
		Tuple("here's some file"),
		Tuple("have a nice day"),
		Tuple("good bye!"),
	}
	// making inputTuples with size 40 * 205 == 8200 bytes, which exceeds a page
	for i := 0; i < 250; i++ {
		inputTuples = append(inputTuples, batch...)
	}

	data, err := ioutil.TempFile("", "gled_ut_tbl_data_*")
	assert.NoError(t, err)
	defer os.Remove(data.Name())
	defer data.Close()

	fsm, err := ioutil.TempFile("", "gled_ut_tbl_fsm_*")
	assert.NoError(t, err)
	defer os.Remove(fsm.Name())
	defer fsm.Close()

	table := NewTable(data, fsm)
	defer table.Close()
	for _, item := range inputTuples {
		err := table.Add(item)
		assert.NoError(t, err)
	}

	outputTuples := []Tuple{}
	err = table.Scan(func(t Tuple, loc TupleLocation) (bool, error) {
		outputTuples = append(outputTuples, t)
		return true, nil
	})
	assert.NoError(t, err)
	assert.EqualValues(t, inputTuples, outputTuples)
}

func TestFsmFreeSpaceToCapacity(t *testing.T) {
	assert.Equal(t, uint8(0), fsmFreeSpaceToCapacity(PageSize))
	assert.Equal(t, uint8(255), fsmFreeSpaceToCapacity(0))
	// remaining space is less than fsmDensity
	// consider a full
	assert.Equal(t, uint8(255), fsmFreeSpaceToCapacity(30))
}

func TestFsmCapacityToFreeSpace(t *testing.T) {
	assert.Equal(t, uint32(PageSize-fsmDensity), fsmCapacityToFreeSpace(0))
}
