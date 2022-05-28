package gled

import "io"

type DataFile interface {
	DataWritable
	DataReadable
}

type DataWritable interface {
	io.Writer
	io.Seeker
}

type DataReadable interface {
	io.Reader
	io.Seeker
}
