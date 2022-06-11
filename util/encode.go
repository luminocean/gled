package util

import (
	"encoding/binary"
	"unsafe"
)

var (
	// Endian is the global endian setting
	Endian = binary.BigEndian
)

func Uint32ToBytes(value uint32) []byte {
	buffer := make([]byte, unsafe.Sizeof(value))
	Endian.PutUint32(buffer, value)
	return buffer
}

func BytesToUint32(data []byte) uint32 {
	return Endian.Uint32(data)
}
