package shardio

import (
	"context"
	"fmt"
	"io"
	"os"
)

// readChunk bounds how many bytes one read issues before re-checking the
// context, so cancellation latency on local files stays below one chunk
// even for very large shards.
const readChunk = 32 * 1024

// FileSource is a filesystem-backed local source. The same type supports
// two capability profiles:
//
//   - KindRandomAccess: every ReadRange opens its own file descriptor and
//     seeks, so several shards of this source can be in flight up to
//     MaxConcurrency (a per-source ceiling independent of the global one).
//   - KindSequential: ReadStream opens a single descriptor and yields bytes
//     in order; the pipeline serializes all shards of this source.
type FileSource struct {
	name string
	path string
	kind Kind
	max  int
}

// NewRandomAccessFileSource builds a seekable source. maxConcurrency is
// the ceiling on simultaneous reads of this file (<1 becomes 1).
func NewRandomAccessFileSource(name, path string, maxConcurrency int) *FileSource {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	return &FileSource{name: name, path: path, kind: KindRandomAccess, max: maxConcurrency}
}

// NewSequentialFileSource builds a streaming source whose shards must be
// consumed by one reader in offset order.
func NewSequentialFileSource(name, path string) *FileSource {
	return &FileSource{name: name, path: path, kind: KindSequential, max: 1}
}

func (s *FileSource) Name() string { return s.name }
func (s *FileSource) Kind() Kind   { return s.kind }
func (s *FileSource) Path() string { return s.path }

func (s *FileSource) MaxConcurrency() int { return s.max }

// Fingerprint binds the run to one file generation: size plus modification
// timestamp plus absolute path. Open/stat permission failures become
// ErrPermissionDenied.
func (s *FileSource) Fingerprint(ctx context.Context) (Fingerprint, error) {
	if err := ctx.Err(); err != nil {
		return Fingerprint{}, err
	}
	fi, err := os.Stat(s.path)
	if err != nil {
		return Fingerprint{}, classifyOSError(err)
	}
	if fi.IsDir() {
		return Fingerprint{}, fmt.Errorf("%w: %q is a directory", ErrInvalidPlan, s.path)
	}
	return Fingerprint{
		Name:   s.name,
		Size:   fi.Size(),
		Stamp:  fi.ModTime().UnixNano(),
		Source: s.path,
	}, nil
}

// ReadRange copies exactly length bytes starting at srcOffset into w using
// a private descriptor (ReadAt never mutates a shared offset). Cancellation
// is checked between chunks; a short read is ErrShardRejected.
func (s *FileSource) ReadRange(ctx context.Context, srcOffset, length int64, w io.Writer) (int64, error) {
	if s.kind != KindRandomAccess {
		return 0, fmt.Errorf("%w: %q is a sequential source", ErrInvalidPlan, s.name)
	}
	f, err := os.OpenFile(s.path, os.O_RDONLY, 0)
	if err != nil {
		return 0, classifyOSError(err)
	}
	defer f.Close()

	buf := make([]byte, readChunk)
	var written int64
	for written < length {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n := int64(len(buf))
		if length-written < n {
			n = length - written
		}
		rn, rerr := f.ReadAt(buf[:n], srcOffset+written)
		if rn > 0 {
			wn, werr := w.Write(buf[:rn])
			written += int64(wn)
			if werr != nil {
				return written, werr
			}
			if wn != rn {
				return written, io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF && written == length {
				break
			}
			if rerr == io.EOF {
				return written, fmt.Errorf("%w: source %q ended after %d/%d bytes at srcOffset=%d",
					ErrShardRejected, s.name, written, length, srcOffset)
			}
			return written, classifyOSError(rerr)
		}
	}
	return written, nil
}

// streamReader is the ReadCloser returned by ReadStream.
type streamReader struct {
	f *os.File
	r io.Reader
}

func (sr *streamReader) Read(p []byte) (int, error) { return sr.r.Read(p) }
func (sr *streamReader) Close() error               { return sr.f.Close() }

// ReadStream opens one descriptor at srcOffset and returns a reader capped
// at length bytes. The pipeline holds MaxConcurrency (always 1) permits for
// the whole lifetime of the reader.
func (s *FileSource) ReadStream(ctx context.Context, srcOffset, length int64) (io.ReadCloser, error) {
	if s.kind != KindSequential {
		return nil, fmt.Errorf("%w: %q is a random-access source; use ReadRange", ErrInvalidPlan, s.name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, classifyOSError(err)
	}
	if _, err := f.Seek(srcOffset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, classifyOSError(err)
	}
	return &streamReader{f: f, r: io.LimitReader(f, length)}, nil
}

var (
	_ RandomAccessSource = (*FileSource)(nil)
	_ SequentialSource   = (*FileSource)(nil)
)
