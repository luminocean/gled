package util

import (
	"bytes"
	"encoding/binary"
)

func Uint32ToBytes(value uint32) []byte {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, value)
	return buffer
}

func BytesToUint32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func Uint64ToBytes(value uint64) []byte {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	return buffer
}

func BytesToUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func Float32ToBytes(value float32) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, value)
	return buf.Bytes()
}

func BytesToFloat32(data []byte) float32 {
	var f float32
	buf := bytes.NewReader(data)
	_ = binary.Read(buf, binary.BigEndian, &f)
	return f
}

func Float64ToBytes(value float64) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, value)
	return buf.Bytes()
}

func BytesToFloat64(data []byte) float64 {
	var f float64
	buf := bytes.NewReader(data)
	_ = binary.Read(buf, binary.BigEndian, &f)
	return f
}
