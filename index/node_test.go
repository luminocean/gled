package index

//func TestLeafNodeInsert(t *testing.T) {
//	leaf := NewLeafNode()
//	err := leaf.Insert(exp.String("key1"), []byte("value"))
//	assert.NoError(t, err)
//	assert.EqualValues(t, []string{"key1"}, leafKeys(leaf))
//
//	err = leaf.Insert(exp.String("key3"), []byte("value"))
//	assert.NoError(t, err)
//	assert.EqualValues(t, []string{"key1", "key3"}, leafKeys(leaf))
//
//	err = leaf.Insert(exp.String("key2"), []byte("value"))
//	assert.NoError(t, err)
//	assert.EqualValues(t, []string{"key1", "key2", "key3"}, leafKeys(leaf))
//
//	err = leaf.Insert(exp.String("key2"), []byte("value2"))
//	assert.NoError(t, err)
//	assert.EqualValues(t, []string{"key1", "key2", "key3"}, leafKeys(leaf))
//}
//
//func TestInternalNodeExpand(t *testing.T) {
//	internal := NewInternalNode()
//	n1 := NewLeafNode()
//	// 10, 20, ..., 100
//	for i := 100; i > 0; i = i - 10 {
//		n2 := NewLeafNode()
//		// key := fmt.Sprintf("%d", i)
//		err := internal.Expand(exp.Int32(i), n1, n2)
//		assert.NoError(t, err)
//		n1 = n2
//	}
//}
//
//func TestBulkInsertion(t *testing.T) {
//	basePayload := "this is payload #"
//	var root Node = NewLeafNode()
//	for i := 0; i < 100000; i++ {
//		key := exp.Int32(i)
//		payload := []byte(fmt.Sprintf("%s%d", basePayload, i))
//		if !root.Fit(key, payload) {
//			mk, right, err := root.Split()
//			assert.NoError(t, err)
//			newRoot := NewInternalNode()
//			err = newRoot.Expand(mk, root, right)
//			assert.NoError(t, err)
//			root = newRoot
//		}
//		err := root.Insert(key, payload)
//		assert.NoError(t, err)
//	}
//	payload, exists, err := root.Lookup(exp.Int32(4200))
//	assert.NoError(t, err)
//	assert.True(t, exists)
//	assert.Equal(t, string(payload), "this is payload #4200")
//}
//
//func leafKeys(node *LeafNode) []string {
//	keys := []string{}
//	for _, item := range node.items {
//		keys = append(keys, string(item.key.(exp.String)))
//	}
//	return keys
//}
