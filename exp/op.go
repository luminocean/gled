package exp

import (
	"errors"
	"fmt"
)

type OpCode string

const (
	ExOpGt  OpCode = "gt"
	ExOpGte OpCode = "gte"
	ExOpLt  OpCode = "lt"
	ExOpLte OpCode = "lte"
	ExOpEq  OpCode = "eq"
	ExOpNeq OpCode = "neq"
)

type OpValue interface {
	IsOpValue()
	Compare(OpValue) (int, error)
}

type OpPrimValue interface {
	OpValue
	IsOpPrimValue()
}

type Int32 int32

func (v Int32) IsOpValue()     {}
func (v Int32) IsOpPrimValue() {}
func (v Int32) Compare(other OpValue) (int, error) {
	return compare[Int32](v, other)
}

type Int64 int64

func (v Int64) IsOpValue()     {}
func (v Int64) IsOpPrimValue() {}
func (v Int64) Compare(other OpValue) (int, error) {
	return compare[Int64](v, other)
}

type Float32 float32

func (v Float32) IsOpValue()     {}
func (v Float32) IsOpPrimValue() {}
func (v Float32) Compare(other OpValue) (int, error) {
	return compare[Float32](v, other)
}

type Float64 float64

func (v Float64) IsOpValue()     {}
func (v Float64) IsOpPrimValue() {}
func (v Float64) Compare(other OpValue) (int, error) {
	return compare[Float64](v, other)
}

type String string

func (v String) IsOpValue()     {}
func (v String) IsOpPrimValue() {}
func (v String) Compare(other OpValue) (int, error) {
	return compare[String](v, other)
}

// Column represents a table column
type Column string

func (c Column) IsOpValue() {}

func C(column string) Column {
	return Column(column)
}

func (c Column) Gt(other any) Ex {
	return ComparisonEx{left: c, op: ExOpGt, right: convertToOpValue(other)}
}

func (c Column) Gte(other any) Ex {
	return ComparisonEx{left: c, op: ExOpGte, right: convertToOpValue(other)}
}

func (c Column) Lt(other any) Ex {
	return ComparisonEx{left: c, op: ExOpLt, right: convertToOpValue(other)}
}

func (c Column) Lte(other any) Ex {
	return ComparisonEx{left: c, op: ExOpLte, right: convertToOpValue(other)}
}

func (c Column) Eq(other any) Ex {
	return ComparisonEx{left: c, op: ExOpEq, right: convertToOpValue(other)}
}

func (c Column) Neq(other any) Ex {
	return ComparisonEx{left: c, op: ExOpNeq, right: convertToOpValue(other)}
}

func (c Column) Compare(other OpValue) (int, error) {
	return 0, errors.New("column comparison is not support yet")
}

func convertToOpValue(value any) OpValue {
	prim := convertToOpPrimValue(value)
	if prim != nil {
		return prim
	}
	if v, ok := value.(Column); ok {
		return v
	}
	return nil
}

func convertToOpPrimValue(value any) OpPrimValue {
	switch v := value.(type) {
	case string:
		return String(v)
	case int:
		return Int32(v)
	case int8:
		return Int32(v)
	case int16:
		return Int32(v)
	case int32:
		return Int32(v)
	case int64:
		return Int64(v)
	case float32:
		return Float32(v)
	case float64:
		return Float64(v)
	default:
		return nil
	}
}

func compare[T Int32 | Int64 | Float32 | Float64 | String](a OpValue, b OpValue) (int, error) {
	v1, ok := a.(T)
	if !ok {
		return 0, fmt.Errorf("unexpect type of %v", a)
	}
	v2, ok := b.(T)
	if !ok {
		return 0, fmt.Errorf("unexpect type of %v", b)
	}
	if v1 == v2 {
		return 0, nil
	} else if v1 < v2 {
		return -1, nil
	} else {
		return 1, nil
	}
}
