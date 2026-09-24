package shardio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFingerprintReflectsFileGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	writeFile(t, path, content(100))
	src := NewRandomAccessFileSource("f", path, 1)
	fp1, err := src.Fingerprint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fp1.Size != 100 || fp1.Name != "f" || fp1.Source != path {
		t.Fatalf("unexpected fingerprint %+v", fp1)
	}
	// Same file, same generation.
	fp2, err := src.Fingerprint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fp1 != fp2 {
		t.Fatalf("fingerprint unstable: %+v vs %+v", fp1, fp2)
	}
	// Append changes size -> new generation.
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fh.Write([]byte("x"))
	_ = fh.Close()
	fp3, err := src.Fingerprint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fp3 == fp1 {
		t.Fatalf("fingerprint did not change after append")
	}
}

func TestFingerprintPermissionDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission checks")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	writeFile(t, path, content(10))
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	src := NewRandomAccessFileSource("f", path, 1)
	_, err := src.Fingerprint(context.Background())
	// Stat itself may succeed with 000 on some systems; the read path must
	// still classify permission failures.
	if err != nil && !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("fingerprint error not classified: %v", err)
	}
	var buf bytes.Buffer
	_, err = src.ReadRange(context.Background(), 0, 10, &buf)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("want ErrPermissionDenied, got %v", err)
	}
}

func TestReadRangeExactAndShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	data := content(1000)
	writeFile(t, path, data)
	src := NewRandomAccessFileSource("f", path, 1)

	var buf bytes.Buffer
	n, err := src.ReadRange(context.Background(), 100, 300, &buf)
	if err != nil || n != 300 {
		t.Fatalf("ReadRange: n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf.Bytes(), data[100:400]) {
		t.Fatalf("range bytes mismatch")
	}

	// Reading past EOF is a shard rejection, not silent truncation.
	buf.Reset()
	_, err = src.ReadRange(context.Background(), 900, 200, &buf)
	if !errors.Is(err, ErrShardRejected) {
		t.Fatalf("want ErrShardRejected on short read, got %v", err)
	}
}

func TestReadRangeCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	writeFile(t, path, content(1<<20))
	src := NewRandomAccessFileSource("f", path, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	_, err := src.ReadRange(ctx, 0, 1<<20, &buf)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestReadStreamCapsLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	data := content(500)
	writeFile(t, path, data)
	src := NewSequentialFileSource("q", path)
	rc, err := src.ReadStream(context.Background(), 50, 120)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[50:170]) {
		t.Fatalf("stream bytes mismatch: got %d bytes", len(got))
	}
}

func TestKindSpecificReadGuards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	writeFile(t, path, content(64))
	seq := NewSequentialFileSource("q", path)
	var buf bytes.Buffer
	if _, err := seq.ReadRange(context.Background(), 0, 10, &buf); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("sequential source must reject ReadRange, got %v", err)
	}
	ra := NewRandomAccessFileSource("r", path, 1)
	if _, err := ra.ReadStream(context.Background(), 0, 10); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("random-access source must reject ReadStream, got %v", err)
	}
}

func TestMaxConcurrencyClamped(t *testing.T) {
	if got := NewRandomAccessFileSource("x", "/nope", 0).MaxConcurrency(); got != 1 {
		t.Fatalf("want clamp to 1, got %d", got)
	}
	if got := NewSequentialFileSource("x", "/nope").MaxConcurrency(); got != 1 {
		t.Fatalf("sequential ceiling must be 1, got %d", got)
	}
}

func TestFingerprintMissingFile(t *testing.T) {
	src := NewRandomAccessFileSource("x", filepath.Join(t.TempDir(), "absent"), 1)
	_, err := src.Fingerprint(context.Background())
	if err == nil || errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("missing file should be a plain error, got %v", err)
	}
}

func TestReadRangeChunkBoundaries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	data := content(readChunk*2 + 7)
	writeFile(t, path, data)
	src := NewRandomAccessFileSource("f", path, 1)
	var buf bytes.Buffer
	start := time.Now()
	n, err := src.ReadRange(context.Background(), 3, int64(len(data))-3, &buf)
	if err != nil || n != int64(len(data))-3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf.Bytes(), data[3:]) {
		t.Fatalf("chunked read mismatch")
	}
	_ = start
}
