package shardio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func writeFile(tb testing.TB, path string, b byte, n int) []byte {
	tb.Helper()
	data := bytes.Repeat([]byte{b}, n)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatal(err)
	}
	return data
}

func readFile(tb testing.TB, path string) []byte {
	tb.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func cfgFor(dir string, specs []SourceSpec, shardSize int64) TaskConfig {
	return TaskConfig{
		TaskID:         "task-" + dir,
		SourceSpecs:    specs,
		OutputPath:     filepath.Join(dir, "out.bin"),
		CheckpointPath: filepath.Join(dir, "ckpt.json"),
		ShardSize:      shardSize,
		Pipeline:       PipelineConfig{GlobalWorkers: 3, InFlightBytes: 200},
		Logger:         quietLogger(),
	}
}

func waitForCompleted(t *testing.T, ckptPath string, atLeast int, deadline time.Duration) {
	t.Helper()
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		cp, err := LoadCheckpoint(ckptPath)
		if err == nil && cp != nil && len(cp.Completed) >= atLeast {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("checkpoint never reached %d completed shards", atLeast)
}

func TestTask_EndToEnd_MixedKinds(t *testing.T) {
	dir := t.TempDir()
	raData := writeFile(t, filepath.Join(dir, "ra.bin"), 'R', 1000)
	sqData := writeFile(t, filepath.Join(dir, "sq.bin"), 'S', 700)
	specs := []SourceSpec{
		{ID: "ra", Path: filepath.Join(dir, "ra.bin"), Kind: RandomAccess, MaxConcurrency: 2},
		{ID: "sq", Path: filepath.Join(dir, "sq.bin"), Kind: Sequential},
	}
	cfg := cfgFor(dir, specs, 128)
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := readFile(t, cfg.OutputPath)
	want := append(append([]byte{}, raData...), sqData...)
	if !bytes.Equal(got, want) {
		t.Fatalf("output len %d != serial concatenation len %d", len(got), len(want))
	}
}

func TestTask_CancelThenResumeTwice(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "big.bin"), 'A', 64*200) // 200 片
	specs := []SourceSpec{{ID: "ra", Path: filepath.Join(dir, "big.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)

	// 第一次：确认若干片后主动取消。
	ctx, cancel := context.WithCancel(context.Background())
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	waitForCompleted(t, cfg.CheckpointPath, 5, 3*time.Second)
	cancel()
	err = <-runErr
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindCanceled {
		t.Fatalf("want KindCanceled, got %#v", err)
	}
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	n1 := len(cp.Completed)
	if n1 < 5 || n1 >= 200 {
		t.Fatalf("unexpected partial progress %d", n1)
	}

	// 第二次：再取消一次，覆盖“恢复后再次中断”。
	ctx2, cancel2 := context.WithCancel(context.Background())
	task2, _ := NewTask(cfg)
	go func() { runErr <- task2.Run(ctx2) }()
	waitForCompleted(t, cfg.CheckpointPath, n1+3, 3*time.Second)
	cancel2()
	if err := <-runErr; !kindIs(err, KindCanceled) {
		t.Fatalf("second cancel: want canceled, got %#v", err)
	}
	cp, _ = LoadCheckpoint(cfg.CheckpointPath)
	n2 := len(cp.Completed)
	if n2 <= n1 || n2 >= 200 {
		t.Fatalf("expected progress beyond %d, got %d", n1, n2)
	}

	// 第三次：恢复到完成，结果与串行读取完全一致，且不重复/丢失。
	task3, _ := NewTask(cfg)
	if err := task3.Run(context.Background()); err != nil {
		t.Fatalf("final resume: %v", err)
	}
	got := readFile(t, cfg.OutputPath)
	if !bytes.Equal(got, data) {
		t.Fatalf("resumed output mismatch: len got=%d want=%d", len(got), len(data))
	}
	cp, _ = LoadCheckpoint(cfg.CheckpointPath)
	if cp.FinalHash == "" {
		t.Fatal("final hash not recorded")
	}
}

func TestTask_RerunAfterCompletionIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'Z', 500)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	t1, _ := NewTask(cfg)
	if err := t1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 完整状态下再次 Run：全部跳过、回读校验通过、幂等成功。
	t2, _ := NewTask(cfg)
	if err := t2.Run(context.Background()); err != nil {
		t.Fatalf("rerun completed task: %v", err)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("output changed on rerun")
	}
}

func TestTask_SourceChangedBetweenRuns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)

	ctx, cancel := context.WithCancel(context.Background())
	t1, _ := NewTask(cfg)
	go func() { _ = t1.Run(ctx) }()
	waitForCompleted(t, cfg.CheckpointPath, 3, 3*time.Second)
	cancel()
	time.Sleep(20 * time.Millisecond)

	// 同大小但内容被改写（指纹哈希必须识别，不能只靠大小）。
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), bytes.Repeat([]byte{'B'}, 640), 0o600); err != nil {
		t.Fatal(err)
	}
	t2, _ := NewTask(cfg)
	err := t2.Run(context.Background())
	if !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("source change must be KindCheckpointMismatch, got %#v", err)
	}
}

func TestTask_CorruptCheckpointFailsClearly(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	t1, _ := NewTask(cfg)
	if err := t1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 物理损坏检查点。
	raw, _ := os.ReadFile(cfg.CheckpointPath)
	if err := os.WriteFile(cfg.CheckpointPath, raw[:len(raw)/3], 0o600); err != nil {
		t.Fatal(err)
	}
	t2, _ := NewTask(cfg)
	if err := t2.Run(context.Background()); !kindIs(err, KindCheckpointCorrupt) {
		t.Fatalf("corrupt checkpoint: want corrupt, got %#v", err)
	}
}

func TestTask_UnsupportedCheckpointVersion(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	t1, _ := NewTask(cfg)
	if err := t1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 篡改版本号字段（不重算哈希，故先构造一个自洽的高版本信封）。
	raw, _ := os.ReadFile(cfg.CheckpointPath)
	tampered := bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":42`), 1)
	_ = os.WriteFile(cfg.CheckpointPath, tampered, 0o600)
	t2, _ := NewTask(cfg)
	err := t2.Run(context.Background())
	// 外层版本不匹配 => KindCheckpointVersion；若只改到 payload 导致哈希失败，则为 corrupt。
	if !kindIs(err, KindCheckpointVersion) && !kindIs(err, KindCheckpointCorrupt) {
		t.Fatalf("future version: want version/corrupt, got %#v", err)
	}
}

func TestTask_PermissionDenied_Source(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test meaningless as root")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "noread.bin")
	writeFile(t, p, 'A', 640)
	if err := os.Chmod(p, 0o000); err != nil {
		t.Fatal(err)
	}
	specs := []SourceSpec{{ID: "a", Path: p, Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	task, _ := NewTask(cfg)
	err := task.Run(context.Background())
	if !kindIs(err, KindPermission) {
		t.Fatalf("unreadable source: want KindPermission, got %#v", err)
	}
}

func TestTask_SequentialSourceResume(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "seq.bin"), 'S', 64*100)
	specs := []SourceSpec{{ID: "s", Path: filepath.Join(dir, "seq.bin"), Kind: Sequential}}
	cfg := cfgFor(dir, specs, 64)

	ctx, cancel := context.WithCancel(context.Background())
	t1, _ := NewTask(cfg)
	go func() { _ = t1.Run(ctx) }()
	waitForCompleted(t, cfg.CheckpointPath, 4, 3*time.Second)
	cancel()
	time.Sleep(20 * time.Millisecond)

	// 恢复：顺序源游标必须越过已确认前缀，不能因游标冲突而失败。
	t2, _ := NewTask(cfg)
	if err := t2.Run(context.Background()); err != nil {
		t.Fatalf("sequential resume: %v", err)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("sequential resumed output mismatch")
	}
}

func TestMergeSerial_ReferenceEquivalence(t *testing.T) {
	dir := t.TempDir()
	raData := writeFile(t, filepath.Join(dir, "r.bin"), 'R', 777)
	sqData := writeFile(t, filepath.Join(dir, "s.bin"), 'S', 333)
	specs := []SourceSpec{
		{ID: "r", Path: filepath.Join(dir, "r.bin"), Kind: RandomAccess, MaxConcurrency: 2},
		{ID: "s", Path: filepath.Join(dir, "s.bin"), Kind: Sequential},
	}
	cfg := cfgFor(dir, specs, 77)
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 用参考串行实现重新读来源，比较输出与最终哈希。
	var srcs []Source
	var states []SourceState
	for _, sp := range specs {
		if sp.Kind == RandomAccess {
			s, err := OpenRandomFile(sp.ID, sp.Path, sp.MaxConcurrency)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.InitFingerprint(context.Background()); err != nil {
				t.Fatal(err)
			}
			srcs = append(srcs, s)
		} else {
			s, err := OpenSequentialFile(sp.ID, sp.Path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.InitFingerprint(context.Background()); err != nil {
				t.Fatal(err)
			}
			srcs = append(srcs, s)
		}
		st := SourceState{ID: srcs[len(srcs)-1].ID(), Kind: srcs[len(srcs)-1].Kind(),
			MaxConcurrency: srcs[len(srcs)-1].MaxConcurrency()}
		fp, _ := srcs[len(srcs)-1].Fingerprint(context.Background())
		st.Fingerprint = fp
		states = append(states, st)
	}
	var serial bytes.Buffer
	serialHash, err := MergeSerial(context.Background(), srcs, states, 77, &serial)
	if err != nil {
		t.Fatal(err)
	}
	got := readFile(t, cfg.OutputPath)
	if !bytes.Equal(got, serial.Bytes()) {
		t.Fatal("pipeline output != MergeSerial reference")
	}
	want := append(append([]byte{}, raData...), sqData...)
	if !bytes.Equal(got, want) {
		t.Fatal("output != raw concatenation")
	}
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	if cp.FinalHash != serialHash {
		t.Fatalf("final hash %s != serial hash %s", cp.FinalHash, serialHash)
	}
}

// stallingSource 前 stallAfter 次读取正常，其后阻塞直到 release 关闭。
type stallingSource struct {
	*RandomFileSource
	started    int64
	stallAfter int64
	release    chan struct{}
	readCount  chan int64
}

func (s *stallingSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	n := atomicIncr(&s.started)
	if s.readCount != nil {
		s.readCount <- n
	}
	if n > s.stallAfter {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, classifyCtxErr(ctx, ctx.Err())
		}
	}
	return s.RandomFileSource.ReadShard(ctx, start, end)
}

func atomicIncr(p *int64) int64 {
	return atomic.AddInt64(p, 1)
}

func makeStallSource(t *testing.T, id, path string, stallAfter int64) *stallingSource {
	t.Helper()
	inner, err := OpenRandomFile(id, path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.InitFingerprint(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &stallingSource{RandomFileSource: inner, stallAfter: stallAfter, release: make(chan struct{})}
}

func TestTask_SourceChangedDuringRun(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 64*400)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	stalled := makeStallSource(t, "a", filepath.Join(dir, "a.bin"), 3)
	cfg.newSource = func(spec SourceSpec) (Source, error) { return stalled, nil }
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(context.Background()) }()
	waitUntil(func() bool { return atomic.LoadInt64(&stalled.started) >= 4 }, 3*time.Second)
	f, err := os.OpenFile(filepath.Join(dir, "a.bin"), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("EXTRA-BYTES"))
	_ = f.Close()
	close(stalled.release)
	err = <-runErr
	if !kindIs(err, KindSourceChanged) {
		t.Fatalf("mid-run source change must be KindSourceChanged, got %#v", err)
	}
}

func TestTask_FailureIsolationOneSourceChanges(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "r.bin"), 'R', 64*400)
	writeFile(t, filepath.Join(dir, "s.bin"), 'S', 64*400)
	specs := []SourceSpec{
		{ID: "r", Path: filepath.Join(dir, "r.bin"), Kind: RandomAccess, MaxConcurrency: 2},
		{ID: "s", Path: filepath.Join(dir, "s.bin"), Kind: RandomAccess, MaxConcurrency: 2},
	}
	cfg := cfgFor(dir, specs, 64)
	stalledS := makeStallSource(t, "s", filepath.Join(dir, "s.bin"), 0)
	cfg.newSource = func(spec SourceSpec) (Source, error) {
		if spec.ID == "s" {
			return stalledS, nil
		}
		inner, err := OpenRandomFile("r", filepath.Join(dir, "r.bin"), 2)
		if err != nil {
			t.Fatal(err)
		}
		if err := inner.InitFingerprint(context.Background()); err != nil {
			t.Fatal(err)
		}
		return inner, nil
	}
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(context.Background()) }()
	waitUntil(func() bool { return atomic.LoadInt64(&stalledS.started) >= 1 }, 3*time.Second)
	f, _ := os.OpenFile(filepath.Join(dir, "s.bin"), os.O_WRONLY|os.O_APPEND, 0o600)
	_, _ = f.Write([]byte("CHANGED"))
	_ = f.Close()
	close(stalledS.release)
	err = <-runErr
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindSourceChanged {
		t.Fatalf("want KindSourceChanged for source s, got %#v", err)
	}
	if e.Source != "s" {
		t.Fatalf("error must identify changed source s, got source=%q", e.Source)
	}
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	if cp != nil && cp.FinalHash != "" {
		t.Fatal("failed task must not record a final hash")
	}
}

func waitUntil(cond func() bool, deadline time.Duration) {
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}
