package index

import (
	"fmt"
	"github.com/luminocean/gled/exp"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)

	// size of an item: 1 + 4 + 4 + size(key) + size(payload) = 1 + 4 + 4 + 8 + 8 = 25
	// size of the header: 4
	// (8192 - 4) / 25 = 327.52 => 327 insertions will ALMOST fill the node
	for i := 0; i < 327; i++ {
		// prefix i with 0 to make it 3 digits
		iStr := fmt.Sprintf("%d", i)
		num := iStr
		for j := 0; j < 3-len(iStr); j++ {
			num = "0" + num
		}
		err = node.Insert(exp.String("hello"+num), []byte("world"+num))
		assert.NoError(t, err)
	}
	err = node.Flush()
	assert.NoError(t, err)

	other := NewLeafNode(f, 0)
	err = other.Initialize()
	assert.NoError(t, err)
	assert.Equal(t, string(other.items[0].Key.(exp.String)), "hello000")
	assert.EqualValues(t, other.items[0].Payload, []byte("world000"))

	assert.Equal(t, string(other.items[9].Key.(exp.String)), "hello009")
	assert.EqualValues(t, other.items[9].Payload, []byte("world009"))

	// node is full now, insert another one
	err = node.Insert(exp.String("hello327"), []byte("world327"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no room for new kv insertion")

	// test splitting
	mk, right, err := node.Split()
	assert.NoError(t, err)
	left := node
	assert.Equal(t, string(mk.(exp.String)), "hello164")
	// left count + right count should equal 327,
	// and left count should be 1 larger than left count (as it includes the middle key)
	assert.Equal(t, 164, len(left.items))
	assert.Equal(t, 163, len(right.(*LeafNode).items))
}
