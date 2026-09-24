package shardio

import (
	"context"
	"io"
)

// Kind classifies a source by its access capability.
type Kind int

const (
	KindRandomAccess Kind = iota // seeks to arbitrary offsets
	KindSequential               // streaming, single in-flight reader
)

// Fingerprint identifies one immutable generation of a source.
type Fingerprint struct {
	Name   string
	Size   int64
	Stamp  int64 // nanoseconds (e.g. mtime)
	Source string
}

// Source is a local data provider participating in a pipeline.
type Source interface {
	Name() string
	Kind() Kind
	Fingerprint(ctx context.Context) (Fingerprint, error)
	MaxConcurrency() int
}

// RandomAccessSource can read arbitrary ranges with its own file handle.
type RandomAccessSource interface {
	Source
	ReadRange(ctx context.Context, offset, length int64, w io.Writer) (int64, error)
}

// SequentialSource yields one ordered stream covering [Offset, Offset+Length).
type SequentialSource interface {
	Source
	ReadStream(ctx context.Context, offset, length int64) (io.ReadCloser, error)
}
