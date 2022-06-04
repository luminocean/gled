package exp

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEval(t *testing.T) {
	data := map[string]any{
		"key1": "value",
		"key2": 100,
		"key3": map[string]any{
			"key4": 10.25,
		},
	}
	assert.True(t, Eval(data, C("key1").Eq("value")))
	assert.True(t, Eval(data, C("key2").Gt(1)))
	assert.True(t, Eval(data, C("key2").Lt(1000000)))
	assert.True(t, Eval(data, C("key3.key4").Gt(5.12)))
	assert.True(t, Eval(data, OrEx{Exps: []Ex{
		C("key1").Eq("another-value"),
		C("key2").Lt(120),
	}}))
	assert.True(t, Eval(data, AndEx{Exps: []Ex{
		C("key1").Eq("value"),
		C("key2").Gt(80),
	}}))
}
