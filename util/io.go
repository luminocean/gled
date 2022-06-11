package util

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// WriteAt writes data into the given position of a file
func WriteAt(file *os.File, data []byte, position int64) (err error) {
	written, err := file.WriteAt(data, position)
	if err != nil {
		return
	} else if written != len(data) {
		err = errors.New("wrong number of bytes written into the page file")
		return
	}
	return
}

// ReadAt reads data at the given position into buffer
func ReadAt(file *os.File, buffer []byte, position int64) (err error) {
	_, err = file.Seek(position, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("failed to seek the page file: %w", err)
		return
	}
	read, err := file.Read(buffer)
	if err != nil {
		// for EOF, return as is since it might be normal sometimes
		if err == io.EOF {
			return err
		}
		err = fmt.Errorf("failed to read data from file: %w", err)
		return
	} else if read != len(buffer) {
		err = fmt.Errorf("mismatched number of bytes read")
		return
	}
	return
}

type Inflatable interface {
	Inflate([]byte) error
}

func ReadObjectAt[T Inflatable](file *os.File, position int64, size int32, target T) (err error) {
	buffer := make([]byte, size)
	err = ReadAt(file, buffer, position)
	if err != nil {
		return
	}
	err = target.Inflate(buffer)
	if err != nil {
		return
	}
	return
}
