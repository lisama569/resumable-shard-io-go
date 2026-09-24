package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- helpers --------------------------------------------------------------

// content generates deterministic pseudo-random bytes for a file of n bytes.
func content(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*2654435761 + n) >> 8)
	}
	return b
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type testLogger struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *testLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format+"\n", args...)
}

func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *testLogger) Contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.buf.String(), s)
}

type fixture struct {
	dir      string
	outPath  string
	ckptPath string
	logger   *testLogger
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	return &fixture{
		dir:      dir,
		outPath:  filepath.Join(dir, "out.bin"),
		ckptPath: filepath.Join(dir, "ckpt.json"),
		logger:   &testLogger{},
	}
}

func (f *fixture) run(t *testing.T, ctx context.Context, plan Plan, conc int) (*Report, error) {
	t.Helper()
	return NewRun(Options{
		Plan:        plan,
		OutputPath:  f.outPath,
		Checkpoints: NewFileCheckpointStore(f.ckptPath),
		Concurrency: conc,
		Logger:      f.logger,
	}).Run(ctx)
}

func goroutines() int {
	// Let background goroutines settle a moment.
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

// ---- basic determinism ----------------------------------------------------

func TestOutputMatchesSerialConcatenation(t *testing.T) {
	f := newFixture(t)
	a, b, c := content(10_000), content(37), content(5_123)
	writeFile(t, filepath.Join(f.dir, "a.bin"), a)
	writeFile(t, filepath.Join(f.dir, "b.bin"), b)
	writeFile(t, filepath.Join(f.dir, "c.bin"), c)

	plan := Plan{
		ShardSize: 1024,
		Sources: []Source{
			NewRandomAccessFileSource("a", filepath.Join(f.dir, "a.bin"), 4),
			NewSequentialFileSource("b", filepath.Join(f.dir, "b.bin")),
			NewRandomAccessFileSource("c", filepath.Join(f.dir, "c.bin"), 2),
		},
	}
	rep, err := f.run(t, context.Background(), plan, 8)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := append(append(append([]byte{}, a...), b...), c...)
	got, err := os.ReadFile(f.outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("merged output differs from serial concatenation")
	}
	if rep.MergedSHA256 != sha256Hex(want) {
		t.Fatalf("merged checksum %s != %s", rep.MergedSHA256, sha256Hex(want))
	}
	if rep.Shards != 10+1+6 {
		t.Fatalf("unexpected shard count %d", rep.Shards)
	}
	if !f.logger.Contains("merged_checksum=") {
		t.Fatalf("expected merged checksum in logs:\n%s", f.logger.String())
	}
}

// ---- resume / idempotency -------------------------------------------------

func TestResumeAfterInterruptionIsIdempotent(t *testing.T) {
	f := newFixture(t)
	data := content(64 * 1024)
	writeFile(t, filepath.Join(f.dir, "src.bin"), data)
	plan := Plan{
		ShardSize: 4096,
		Sources:   []Source{NewRandomAccessFileSource("s", filepath.Join(f.dir, "src.bin"), 4)},
	}

	// First run: cancel partway through by killing the context once the
	// first shard has been confirmed.
	ctx, cancel := context.WithCancel(context.Background())
	stopper := &cancelAfterConfirms{after: 2, cancel: cancel, logger: f.logger}
	rep, err := NewRun(Options{
		Plan:        plan,
		OutputPath:  f.outPath,
		Checkpoints: NewFileCheckpointStore(f.ckptPath),
		Concurrency: 4,
		Logger:      stopper,
	}).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected cancellation, got rep=%v err=%v", rep, err)
	}

	cp, err := NewFileCheckpointStore(f.ckptPath).Load(context.Background())
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if len(cp.Completed) == 0 || cp.Finished {
		t.Fatalf("expected partial checkpoint, got completed=%d finished=%v",
			len(cp.Completed), cp.Finished)
	}

	// Second run resumes: previously confirmed shards are reused, the rest
	// are processed exactly once.
	rep, err = f.run(t, context.Background(), plan, 4)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if rep.Reused != len(cp.Completed) {
		t.Fatalf("reused=%d, want %d", rep.Reused, len(cp.Completed))
	}
	if rep.Processed != rep.Shards-rep.Reused {
		t.Fatalf("processed=%d reused=%d shards=%d", rep.Processed, rep.Reused, rep.Shards)
	}
	got, err := os.ReadFile(f.outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("resumed output differs from source")
	}
	if !f.logger.Contains("basis:") {
		t.Fatalf("expected decision basis in logs:\n%s", f.logger.String())
	}

	// Third run: duplicate submission of a finished run is a no-op.
	rep, err = f.run(t, context.Background(), plan, 4)
	if err != nil {
		t.Fatalf("duplicate run: %v", err)
	}
	if rep.Processed != 0 || rep.Skipped != rep.Shards {
		t.Fatalf("duplicate run should skip all shards, got %+v", rep)
	}
}

// cancelAfterConfirms cancels the context after n shard confirmations.
type cancelAfterConfirms struct {
	after  int
	cancel context.CancelFunc
	logger *testLogger
	n      int32
}

func (c *cancelAfterConfirms) Printf(format string, args ...any) {
	c.logger.Printf(format, args...)
	if strings.Contains(format, "confirmed") {
		if atomic.AddInt32(&c.n, 1) == int32(c.after) {
			c.cancel()
		}
	}
}

func TestCheckpointSurvivesKillBetweenFsyncAndSave(t *testing.T) {
	f := newFixture(t)
	data := content(8 * 1024)
	writeFile(t, filepath.Join(f.dir, "src.bin"), data)
	plan := Plan{
		ShardSize: 1024,
		Sources:   []Source{NewRandomAccessFileSource("s", filepath.Join(f.dir, "src.bin"), 2)},
	}
	// A store that "crashes" after the third save: the process dies with a
	// consistent (older) checkpoint, never a torn one.
	store := &crashyStore{inner: NewFileCheckpointStore(f.ckptPath), crashAfter: 3}
	_, err := NewRun(Options{
		Plan:        plan,
		OutputPath:  f.outPath,
		Checkpoints: store,
		Concurrency: 2,
		Logger:      f.logger,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "simulated crash") {
		t.Fatalf("expected simulated crash, got %v", err)
	}
	// The on-disk checkpoint must still decode and be internally consistent.
	cp, err := NewFileCheckpointStore(f.ckptPath).Load(context.Background())
	if err != nil {
		t.Fatalf("checkpoint after crash must be valid: %v", err)
	}
	if len(cp.Completed) != 3 {
		t.Fatalf("expected 3 durable confirmations, got %d", len(cp.Completed))
	}
	// Resume with a healthy store completes the run.
	rep, err := f.run(t, context.Background(), plan, 2)
	if err != nil {
		t.Fatalf("resume after crash: %v", err)
	}
	if rep.Reused != 3 {
		t.Fatalf("reused=%d, want 3", rep.Reused)
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(data) {
		t.Fatalf("output mismatch after crash resume")
	}
}

type crashyStore struct {
	inner      *FileCheckpointStore
	crashAfter int
	saves      int
}

func (s *crashyStore) Load(ctx context.Context) (*Checkpoint, error) {
	return s.inner.Load(ctx)
}

func (s *crashyStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := s.inner.Save(ctx, cp); err != nil {
		return err
	}
	s.saves++
	if s.saves >= s.crashAfter {
		return fmt.Errorf("simulated crash after durable save %d", s.saves)
	}
	return nil
}

// ---- concurrency ----------------------------------------------------------

// slowRA wraps a random-access source with a per-read delay and tracks the
// maximum number of concurrent reads actually observed.
type slowRA struct {
	inner  RandomAccessSource
	delay  time.Duration
	cur    atomic.Int32
	maxObs atomic.Int32
}

func (s *slowRA) Name() string        { return s.inner.Name() }
func (s *slowRA) Kind() Kind          { return s.inner.Kind() }
func (s *slowRA) MaxConcurrency() int { return s.inner.MaxConcurrency() }
func (s *slowRA) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return s.inner.Fingerprint(ctx)
}
func (s *slowRA) ReadRange(ctx context.Context, off, length int64, w io.Writer) (int64, error) {
	cur := s.cur.Add(1)
	for {
		m := s.maxObs.Load()
		if cur <= m || s.maxObs.CompareAndSwap(m, cur) {
			break
		}
	}
	defer s.cur.Add(-1)
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return s.inner.ReadRange(ctx, off, length, w)
}

func TestConcurrencyCeilingsAreRespected(t *testing.T) {
	f := newFixture(t)
	data := content(96 * 1024)
	writeFile(t, filepath.Join(f.dir, "src.bin"), data)
	slow := &slowRA{
		inner: NewRandomAccessFileSource("s", filepath.Join(f.dir, "src.bin"), 2),
		delay: 5 * time.Millisecond,
	}
	plan := Plan{ShardSize: 2048, Sources: []Source{slow}}
	before := goroutines()
	rep, err := f.run(t, context.Background(), plan, 16)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Processed != rep.Shards {
		t.Fatalf("not all shards processed: %+v", rep)
	}
	// Per-source ceiling (2) must bind even though the global ceiling is 16.
	if got := slow.maxObs.Load(); got > 2 {
		t.Fatalf("per-source concurrency ceiling violated: observed %d", got)
	}
	if got := slow.maxObs.Load(); got < 2 {
		t.Fatalf("expected to actually reach the ceiling of 2, observed %d", got)
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(data) {
		t.Fatalf("output mismatch")
	}
	after := goroutines()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestSequentialSourceIsProcessedInOrder(t *testing.T) {
	f := newFixture(t)
	data := content(20 * 1024)
	writeFile(t, filepath.Join(f.dir, "seq.bin"), data)
	rec := &recordingSeq{inner: NewSequentialFileSource("q", filepath.Join(f.dir, "seq.bin"))}
	plan := Plan{ShardSize: 1024, Sources: []Source{rec}}
	rep, err := f.run(t, context.Background(), plan, 8)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Processed != 20 {
		t.Fatalf("processed=%d, want 20", rep.Processed)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.offsets) != 20 {
		t.Fatalf("expected 20 stream opens, got %d", len(rec.offsets))
	}
	for i, off := range rec.offsets {
		if want := int64(i) * 1024; off != want {
			t.Fatalf("stream %d opened at offset %d, want %d (out of order)", i, off, want)
		}
	}
	if rec.maxOpen > 1 {
		t.Fatalf("sequential source had %d concurrent streams", rec.maxOpen)
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(data) {
		t.Fatalf("output mismatch")
	}
}

type recordingSeq struct {
	inner   SequentialSource
	mu      sync.Mutex
	offsets []int64
	open    int
	maxOpen int
}

func (r *recordingSeq) Name() string        { return r.inner.Name() }
func (r *recordingSeq) Kind() Kind          { return r.inner.Kind() }
func (r *recordingSeq) MaxConcurrency() int { return r.inner.MaxConcurrency() }
func (r *recordingSeq) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return r.inner.Fingerprint(ctx)
}
func (r *recordingSeq) ReadStream(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.offsets = append(r.offsets, off)
	r.open++
	if r.open > r.maxOpen {
		r.maxOpen = r.open
	}
	r.mu.Unlock()
	rc, err := r.inner.ReadStream(ctx, off, length)
	if err != nil {
		r.mu.Lock()
		r.open--
		r.mu.Unlock()
		return nil, err
	}
	return &trackedRC{ReadCloser: rc, done: func() {
		r.mu.Lock()
		r.open--
		r.mu.Unlock()
	}}, nil
}

type trackedRC struct {
	io.ReadCloser
	done func()
}

func (t *trackedRC) Close() error {
	err := t.ReadCloser.Close()
	t.done()
	return err
}

// ---- cancellation ---------------------------------------------------------

func TestCancellationLeavesNoUnfinishedWork(t *testing.T) {
	f := newFixture(t)
	data := content(256 * 1024)
	writeFile(t, filepath.Join(f.dir, "src.bin"), data)
	slow := &slowRA{
		inner: NewRandomAccessFileSource("s", filepath.Join(f.dir, "src.bin"), 4),
		delay: 50 * time.Millisecond,
	}
	plan := Plan{ShardSize: 4096, Sources: []Source{slow}}
	ctx, cancel := context.WithCancel(context.Background())
	before := goroutines()
	done := make(chan error, 1)
	go func() {
		_, err := f.run(t, ctx, plan, 8)
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("expected ErrCanceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop promptly after cancellation")
	}
	// No goroutines may linger, and the checkpoint must remain loadable.
	if after := goroutines(); after > before+2 {
		t.Fatalf("goroutine leak after cancel: before=%d after=%d", before, after)
	}
	if _, err := NewFileCheckpointStore(f.ckptPath).Load(context.Background()); err != nil {
		t.Fatalf("checkpoint unreadable after cancel: %v", err)
	}
	// A fresh run on the same plan resumes cleanly.
	rep, err := f.run(t, context.Background(), plan, 8)
	if err != nil {
		t.Fatalf("resume after cancel: %v", err)
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(data) {
		t.Fatalf("output mismatch after cancel+resume (rep=%+v)", rep)
	}
}

func TestDeadlineExceededIsCanceled(t *testing.T) {
	f := newFixture(t)
	data := content(64 * 1024)
	writeFile(t, filepath.Join(f.dir, "src.bin"), data)
	slow := &slowRA{
		inner: NewRandomAccessFileSource("s", filepath.Join(f.dir, "src.bin"), 2),
		delay: 200 * time.Millisecond,
	}
	plan := Plan{ShardSize: 4096, Sources: []Source{slow}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := f.run(t, ctx, plan, 4)
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected ErrCanceled on deadline, got %v", err)
	}
}

// ---- source changes -------------------------------------------------------

func TestSourceChangeDuringRunAborts(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.dir, "src.bin")
	writeFile(t, path, content(64*1024))
	// Deterministic mid-run mutation: the store rewrites the source file
	// after the third durable confirmation, while later shards are still
	// pending. The next generation check must abort the run.
	mutate := func() {
		fh, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return
		}
		_, _ = fh.Write([]byte("tampered"))
		_ = fh.Close()
	}
	store := &mutatingStore{
		inner:       NewFileCheckpointStore(f.ckptPath),
		mutate:      mutate,
		mutateAfter: 3,
	}
	plan := Plan{
		ShardSize: 2048,
		Sources:   []Source{NewRandomAccessFileSource("s", path, 2)},
	}
	_, err := NewRun(Options{
		Plan:        plan,
		OutputPath:  f.outPath,
		Checkpoints: store,
		Concurrency: 2,
		Logger:      f.logger,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), string(ErrSourceChanged)) {
		t.Fatalf("expected ErrSourceChanged, got %v", err)
	}
	if !f.logger.Contains("bound to") {
		t.Fatalf("expected generation basis in logs:\n%s", f.logger.String())
	}
	// Restarting with the same (now changed) source must NOT silently
	// succeed against the old checkpoint: it is a checkpoint/source
	// mismatch, reported distinctly.
	_, err = f.run(t, context.Background(), plan, 2)
	if err == nil || !strings.Contains(err.Error(), string(ErrCheckpointMismatch)) {
		t.Fatalf("expected ErrCheckpointMismatch on restart, got %v", err)
	}
}

// mutatingStore mutates the source after mutateAfter durable saves.
type mutatingStore struct {
	inner       *FileCheckpointStore
	mutate      func()
	mutateAfter int
	saves       int
	mutated     bool
}

func (m *mutatingStore) Load(ctx context.Context) (*Checkpoint, error) {
	return m.inner.Load(ctx)
}

func (m *mutatingStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := m.inner.Save(ctx, cp); err != nil {
		return err
	}
	m.saves++
	if m.saves >= m.mutateAfter && !m.mutated {
		m.mutated = true
		m.mutate()
	}
	return nil
}

func TestSequentialSourceChangeDuringRunAborts(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.dir, "seq.bin")
	writeFile(t, path, content(32*1024))
	mutator := &mutatingSeq{
		inner: NewSequentialFileSource("q", path),
		mutate: func() {
			fh, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return
			}
			_, _ = fh.Write([]byte("tampered"))
			_ = fh.Close()
		},
		triggerAfter: 3,
	}
	plan := Plan{ShardSize: 1024, Sources: []Source{mutator}}
	_, err := f.run(t, context.Background(), plan, 4)
	if err == nil || !strings.Contains(err.Error(), string(ErrSourceChanged)) {
		t.Fatalf("expected ErrSourceChanged, got %v", err)
	}
}

type mutatingSeq struct {
	inner        SequentialSource
	mutate       func()
	triggerAfter int
	opens        atomic.Int32
	mutated      atomic.Bool
}

func (m *mutatingSeq) Name() string        { return m.inner.Name() }
func (m *mutatingSeq) Kind() Kind          { return m.inner.Kind() }
func (m *mutatingSeq) MaxConcurrency() int { return m.inner.MaxConcurrency() }
func (m *mutatingSeq) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return m.inner.Fingerprint(ctx)
}
func (m *mutatingSeq) ReadStream(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	n := m.opens.Add(1)
	if n >= int32(m.triggerAfter) && m.mutated.CompareAndSwap(false, true) {
		m.mutate()
	}
	return m.inner.ReadStream(ctx, off, length)
}

// ---- permissions ----------------------------------------------------------

func TestPermissionDeniedIsClassified(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission checks")
	}
	f := newFixture(t)
	path := filepath.Join(f.dir, "secret.bin")
	writeFile(t, path, content(4096))
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	plan := Plan{
		ShardSize: 1024,
		Sources:   []Source{NewRandomAccessFileSource("s", path, 1)},
	}
	_, err := f.run(t, context.Background(), plan, 1)
	if err == nil || !strings.Contains(err.Error(), string(ErrPermissionDenied)) {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

func TestSequentialPermissionDeniedIsClassified(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission checks")
	}
	f := newFixture(t)
	path := filepath.Join(f.dir, "secret.bin")
	writeFile(t, path, content(4096))
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	plan := Plan{
		ShardSize: 1024,
		Sources:   []Source{NewSequentialFileSource("q", path)},
	}
	_, err := f.run(t, context.Background(), plan, 1)
	if err == nil || !strings.Contains(err.Error(), string(ErrPermissionDenied)) {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

// ---- mixed sources --------------------------------------------------------

func TestMixedSourcesCooperate(t *testing.T) {
	f := newFixture(t)
	a, b := content(33_000), content(7_777)
	writeFile(t, filepath.Join(f.dir, "ra.bin"), a)
	writeFile(t, filepath.Join(f.dir, "seq.bin"), b)
	plan := Plan{
		ShardSize: 1500,
		Sources: []Source{
			NewRandomAccessFileSource("ra", filepath.Join(f.dir, "ra.bin"), 3),
			NewSequentialFileSource("seq", filepath.Join(f.dir, "seq.bin")),
		},
	}
	rep, err := f.run(t, context.Background(), plan, 6)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := append(append([]byte{}, a...), b...)
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(want) {
		t.Fatalf("mixed-source merge mismatch")
	}
	if rep.MergedSHA256 != sha256Hex(want) {
		t.Fatalf("checksum mismatch")
	}
}

// ---- edge cases -----------------------------------------------------------

func TestEmptySourceProducesNoShards(t *testing.T) {
	f := newFixture(t)
	writeFile(t, filepath.Join(f.dir, "empty.bin"), nil)
	writeFile(t, filepath.Join(f.dir, "one.bin"), content(10))
	plan := Plan{
		ShardSize: 4,
		Sources: []Source{
			NewRandomAccessFileSource("e", filepath.Join(f.dir, "empty.bin"), 1),
			NewSequentialFileSource("o", filepath.Join(f.dir, "one.bin")),
		},
	}
	rep, err := f.run(t, context.Background(), plan, 2)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Shards != 3 {
		t.Fatalf("want 3 shards for 10 bytes, got %d", rep.Shards)
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(content(10)) {
		t.Fatalf("empty source must contribute zero bytes")
	}
}

func TestInvalidPlans(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	writeFile(t, path, content(10))
	src := NewRandomAccessFileSource("s", path, 1)
	cases := map[string]Plan{
		"zero shard size": {ShardSize: 0, Sources: []Source{src}},
		"no sources":      {ShardSize: 1},
		"empty name":      {ShardSize: 1, Sources: []Source{NewRandomAccessFileSource("", path, 1)}},
	}
	for name, plan := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.run(t, context.Background(), plan, 1)
			if err == nil || !strings.Contains(err.Error(), string(ErrInvalidPlan)) {
				t.Fatalf("want ErrInvalidPlan, got %v", err)
			}
		})
	}
}

func TestDuplicateSourceNamesRejected(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f.bin")
	writeFile(t, p, content(8))
	plan := Plan{
		ShardSize: 4,
		Sources: []Source{
			NewRandomAccessFileSource("dup", p, 1),
			NewSequentialFileSource("dup", p),
		},
	}
	f := newFixture(t)
	_, err := f.run(t, context.Background(), plan, 1)
	if err == nil || !strings.Contains(err.Error(), string(ErrInvalidPlan)) {
		t.Fatalf("want ErrInvalidPlan for duplicate names, got %v", err)
	}
}

func TestOutputOverlappingSourceRejected(t *testing.T) {
	f := newFixture(t)
	path := f.outPath // output IS the source
	writeFile(t, path, content(16))
	plan := Plan{
		ShardSize: 8,
		Sources:   []Source{NewRandomAccessFileSource("s", path, 1)},
	}
	_, err := NewRun(Options{
		Plan:        plan,
		OutputPath:  path,
		Checkpoints: NewFileCheckpointStore(f.ckptPath),
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), string(ErrInvalidPlan)) {
		t.Fatalf("want ErrInvalidPlan for output/source overlap, got %v", err)
	}
}

func TestTornOutputRegionIsReprocessedOnResume(t *testing.T) {
	f := newFixture(t)
	data := content(16 * 1024)
	path := filepath.Join(f.dir, "src.bin")
	writeFile(t, path, data)
	plan := Plan{
		ShardSize: 4096,
		Sources:   []Source{NewRandomAccessFileSource("s", path, 2)},
	}
	// Stop after two confirmations.
	ctx, cancel := context.WithCancel(context.Background())
	stopper := &cancelAfterConfirms{after: 2, cancel: cancel, logger: f.logger}
	_, err := NewRun(Options{
		Plan:        plan,
		OutputPath:  f.outPath,
		Checkpoints: NewFileCheckpointStore(f.ckptPath),
		Concurrency: 2,
		Logger:      stopper,
	}).Run(ctx)
	if !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected cancel, got %v", err)
	}
	// Corrupt one confirmed region directly in the output file.
	fh, err := os.OpenFile(f.outPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteAt([]byte("DESTROYED"), 0); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	rep, err := f.run(t, context.Background(), plan, 2)
	if err != nil {
		t.Fatalf("resume with torn region: %v", err)
	}
	// Exactly one reused region was invalidated and reprocessed.
	// The cancel races the workers: 2 shards were requested confirmed, a
	// third confirmation may land first. Shard 0 is always among the first
	// two confirmed, so exactly its region is invalidated; only the totals
	// are deterministic.
	if rep.Reused+rep.Processed != 4 || rep.Processed != 4-rep.Reused || rep.Reused < 1 {
		head, _ := os.ReadFile(f.outPath)
		t.Fatalf("inconsistent counts %+v\nhead=%q\nlogs:\n%s", rep, head[:16], f.logger.String())
	}
	got, _ := os.ReadFile(f.outPath)
	if string(got) != string(data) {
		t.Fatalf("output not repaired")
	}
	if !f.logger.Contains("reprocess") || !f.logger.Contains("output readback") {
		t.Fatalf("expected reprocess basis in logs:\n%s", f.logger)
	}
}

func TestStressManyRunsRace(t *testing.T) {
	for iter := 0; iter < 3; iter++ {
		f := newFixture(t)
		path := filepath.Join(f.dir, "src.bin")
		data := content(120 * 1024)
		writeFile(t, path, data)
		plan := Plan{
			ShardSize: 777, // awkward boundary: 120000/777 is not integral
			Sources:   []Source{NewRandomAccessFileSource("s", path, 3)},
		}
		rep, err := f.run(t, context.Background(), plan, 7)
		if err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		got, _ := os.ReadFile(f.outPath)
		if string(got) != string(data) {
			t.Fatalf("iter %d mismatch", iter)
		}
		if rep.MergedSHA256 != sha256Hex(data) {
			t.Fatalf("iter %d checksum", iter)
		}
	}
}
