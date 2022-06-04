package exp

import (
	"reflect"
	"strings"
)

// Eval evaluates whether the given data matches query expressions
func Eval(data map[string]any, exp Ex) bool {
	switch expression := exp.(type) {
	case ComparisonEx:
		return evalField(data, expression.left, expression.op, expression.right)
	case OrEx:
		// true for empty exp
		if len(expression.Exps) == 0 {
			return true
		}
		for _, ex := range expression.Exps {
			if Eval(data, ex) {
				return true
			}
		}
		return false
	case AndEx:
		for _, ex := range expression.Exps {
			if !Eval(data, ex) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func evalField(data map[string]any, left OpValue, opCode OpCode, right OpValue) bool {
	var lopv, ropv OpPrimValue
	lopv, found := resolveToPrimValue(data, left)
	if !found {
		return false
	}
	ropv, found = resolveToPrimValue(data, right)
	if !found {
		return false
	}
	if reflect.TypeOf(lopv) != reflect.TypeOf(ropv) {
		return false
	}
	switch lv := lopv.(type) {
	case String:
		return compareField(lv, opCode, ropv.(String))
	case Int32:
		return compareField(lv, opCode, ropv.(Int32))
	case Int64:
		return compareField(lv, opCode, ropv.(Int64))
	case Float32:
		return compareField(lv, opCode, ropv.(Float32))
	case Float64:
		return compareField(lv, opCode, ropv.(Float64))
	default:
		return false
	}
}

func compareField[T String | Int32 | Int64 | Float32 | Float64](left T, opCode OpCode, right T) bool {
	switch opCode {
	case ExOpGt:
		return left > right
	case ExOpGte:
		return left >= right
	case ExOpLt:
		return left < right
	case ExOpLte:
		return left <= right
	case ExOpEq:
		return left == right
	case ExOpNeq:
		return left != right
	default:
		return false
	}
}

func resolveToPrimValue(data map[string]any, value OpValue) (OpPrimValue, bool) {
	var opv OpPrimValue
	if v, ok := value.(Column); ok {
		var found bool
		opv, found = readColumn(data, v)
		if !found {
			return nil, false
		}
	} else if v, ok := value.(OpPrimValue); ok {
		opv = v
	}
	return opv, true
}

// read a primitive value from the data map
func readColumn(data map[string]any, key Column) (OpPrimValue, bool) {
	// value to be compared with
	var value any
	for _, segment := range strings.Split(string(key), ".") {
		// value already resolved, cannot accept more sub paths
		if value != nil {
			return nil, false
		}
		if next, exists := data[segment]; exists {
			if m, ok := next.(map[string]any); ok {
				data = m
			} else {
				value = next
			}
		} else {
			return nil, false
		}
	}
	var opv OpPrimValue
	opv = convertToOpPrimValue(value)
	if opv != nil {
		return opv, true
	} else {
		return nil, false
	}
}
