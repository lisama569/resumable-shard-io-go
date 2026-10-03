package shardio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// fileFingerprint 简单记录文件大小与内容，供“体检不得写入”断言。
func fileFingerprint(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return checksumBytes(b)
}

func newCompletedTask(t *testing.T, dir string, specs []SourceSpec, shardSize int64, nByte int) TaskConfig {
	t.Helper()
	cfg := cfgFor(dir, specs, shardSize)
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	return cfg
}

func TestInspect_HealthyTask(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)

	beforeOut := fileFingerprint(t, cfg.OutputPath)
	beforeCp := fileFingerprint(t, cfg.CheckpointPath)

	task, _ := NewTask(cfg)
	rep, err := task.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !rep.Healthy() {
		t.Fatalf("fresh task must be healthy: damaged=%v changed=%v", rep.Damaged, rep.ChangedSources)
	}
	if len(rep.Shards) != 10 {
		t.Fatalf("want 10 inspected shards, got %d", len(rep.Shards))
	}
	for _, h := range rep.Shards {
		if h.Verdict != ShardOK {
			t.Fatalf("shard %d verdict=%s, want ok (%s)", h.Index, h.Verdict, h.Reason)
		}
	}
	// 只读：输出与检查点字节不得有任何变化。
	if after := fileFingerprint(t, cfg.OutputPath); after != beforeOut {
		t.Fatal("inspect modified the output file")
	}
	if after := fileFingerprint(t, cfg.CheckpointPath); after != beforeCp {
		t.Fatal("inspect modified the checkpoint file")
	}
}

func TestInspect_DetectsCorruptedAndTruncatedOutput(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)

	// 改坏第 3 片输出区间 [192,256)，其余不动。
	out, err := os.OpenFile(cfg.OutputPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := out.WriteAt([]byte("CORRUPTED!!"), 200); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask(cfg)
	rep, err := task.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if rep.Healthy() {
		t.Fatal("corruption must be detected")
	}
	if !rep.Damaged[3] || len(rep.Damaged) != 1 {
		t.Fatalf("want exactly damaged shard 3, got %v", rep.Damaged)
	}
	if rep.Shards[3].Verdict != ShardChecksumMismatch {
		t.Fatalf("shard 3 verdict=%s want checksum_mismatch", rep.Shards[3].Verdict)
	}
	if rep.Shards[3].Output == "" || rep.Shards[3].Output == rep.Shards[3].Recorded {
		t.Fatal("health record must carry differing output/recorded checksums as evidence")
	}

	// 截断到 400 字节：第 6..9 片装不下，应为 output_missing；第 3 片仍为 checksum_mismatch。
	if err := os.Truncate(cfg.OutputPath, 400); err != nil {
		t.Fatal(err)
	}
	task2, _ := NewTask(cfg)
	rep2, err := task2.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect after truncate: %v", err)
	}
	for idx := 6; idx <= 9; idx++ {
		if !rep2.Damaged[idx] {
			t.Fatalf("truncated shard %d must be damaged", idx)
		}
		if rep2.Shards[idx].Verdict != ShardOutputMissing {
			t.Fatalf("shard %d verdict=%s want output_missing", idx, rep2.Shards[idx].Verdict)
		}
	}
	if rep2.Shards[3].Verdict != ShardChecksumMismatch {
		t.Fatalf("shard 3 still checksum_mismatch, got %s", rep2.Shards[3].Verdict)
	}
	for idx := 0; idx <= 2; idx++ {
		if rep2.Damaged[idx] {
			t.Fatalf("untouched shard %d must remain ok", idx)
		}
	}
	_ = data
}

func TestInspect_OutputFileDeleted(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)
	if err := os.Remove(cfg.OutputPath); err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask(cfg)
	rep, err := task.Inspect(context.Background())
	if err != nil {
		t.Fatalf("missing output is a reportable condition, not an error: %v", err)
	}
	if len(rep.Damaged) != 10 {
		t.Fatalf("want all 10 shards missing, got damaged=%v", rep.Damaged)
	}
	for _, h := range rep.Shards {
		if h.Verdict != ShardOutputMissing {
			t.Fatalf("shard %d verdict=%s", h.Index, h.Verdict)
		}
	}
}

func TestInspect_ChangedSameSizeSource(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)
	// 同大小改写：输出仍然自洽，但来源指纹必须报变化。
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), bytes.Repeat([]byte{'B'}, 640), 0o600); err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask(cfg)
	rep, err := task.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !rep.ChangedSources["a"] {
		t.Fatal("changed source must be reported")
	}
	if len(rep.Damaged) != 0 {
		t.Fatalf("output shards still match checkpoint; damaged=%v", rep.Damaged)
	}
	var sh *SourceHealth
	for i := range rep.Sources {
		if rep.Sources[i].ID == "a" {
			sh = &rep.Sources[i]
		}
	}
	if sh == nil || sh.Matches || sh.Reason == "" {
		t.Fatal("source health must explain the fingerprint difference")
	}
}

func TestInspect_NoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)
	task, _ := NewTask(cfg)
	_, err := task.Inspect(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindNothingToRepair {
		t.Fatalf("want KindNothingToRepair, got %#v", err)
	}
	if !errors.Is(err, ErrNothingToRepair) {
		t.Fatalf("errors.Is ErrNothingToRepair failed: %#v", err)
	}
}

// runRecover 执行一次修复并返回报告。
func runRecover(t *testing.T, cfg TaskConfig, opts RecoverOptions) *RecoverReport {
	t.Helper()
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := task.Recover(context.Background(), opts)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	return rep
}

func TestRecover_CorruptedOutputRedoneOnly(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)

	// 改坏第 3、7 片输出区间。
	for _, off := range []int64{192, 448} {
		f, err := os.OpenFile(cfg.OutputPath, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{'X', 'X'}, off); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	// 先体检，拿到精确坏片集合。
	insp, _ := NewTask(cfg)
	rep0, err := insp.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep0.Damaged) != 2 || !rep0.Damaged[3] || !rep0.Damaged[7] {
		t.Fatalf("want damaged {3,7}, got %v", rep0.Damaged)
	}

	rep := runRecover(t, cfg, RecoverOptions{})
	if !intsEqualSet(rep.Repaired, []int{3, 7}) {
		t.Fatalf("want repaired [3 7], got %v", rep.Repaired)
	}
	if len(rep.Reused) != 8 {
		t.Fatalf("want 8 reused, got %d (%v)", len(rep.Reused), rep.Reused)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("repaired output differs from full serial rerun")
	}
	// 修复后体检必须全绿。
	insp2, _ := NewTask(cfg)
	after, err := insp2.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !after.Healthy() {
		t.Fatalf("post-repair inspection must be healthy: %v %v", after.Damaged, after.ChangedSources)
	}
}

func TestRecover_DeletedOutputRebuiltAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)
	if err := os.Remove(cfg.OutputPath); err != nil {
		t.Fatal(err)
	}
	rep := runRecover(t, cfg, RecoverOptions{})
	if len(rep.Repaired) != 10 {
		t.Fatalf("all 10 shards must be redone, got %v", rep.Repaired)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("rebuilt output mismatch")
	}

	// 第二次修复：没有任何坏片，全部复用、不触碰数据。
	before := fileFingerprint(t, cfg.OutputPath)
	rep2 := runRecover(t, cfg, RecoverOptions{})
	if len(rep2.Repaired) != 0 || len(rep2.Reused) != 10 {
		t.Fatalf("second repair must be a no-op: repaired=%v reused=%d", rep2.Repaired, len(rep2.Reused))
	}
	if after := fileFingerprint(t, cfg.OutputPath); after != before {
		t.Fatal("idempotent repair must leave output byte-identical")
	}
}

func TestRecover_TruncatedOutputSequentialSource(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "s.bin"), 'S', 64*12)
	specs := []SourceSpec{{ID: "s", Path: filepath.Join(dir, "s.bin"), Kind: Sequential}}
	cfg := newCompletedTask(t, dir, specs, 64, 64*12)
	// 截断到 5 片（320B）。
	if err := os.Truncate(cfg.OutputPath, 64*5); err != nil {
		t.Fatal(err)
	}
	rep := runRecover(t, cfg, RecoverOptions{})
	if !intsEqualSet(rep.Repaired, []int{5, 6, 7, 8, 9, 10, 11}) {
		t.Fatalf("want repaired tail 5..11, got %v", rep.Repaired)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("sequential repaired output mismatch")
	}
}

func intsEqualSet(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	m := map[int]bool{}
	for _, v := range got {
		m[v] = true
	}
	for _, v := range want {
		if !m[v] {
			return false
		}
	}
	return true
}

// countingRandomSource 统计 ReadShard 调用次数（用于断言“未受影响片不重读”）。
type countingRandomSource struct {
	*RandomFileSource
	reads atomic.Int64
}

func (c *countingRandomSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	c.reads.Add(1)
	return c.RandomFileSource.ReadShard(ctx, start, end)
}

func TestRecover_SameSizeSourceChangeIncremental(t *testing.T) {
	dir := t.TempDir()
	orig := bytes.Repeat([]byte{'A'}, 640)
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), orig, 0o600); err != nil {
		t.Fatal(err)
	}
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)

	// 同大小改写：只改第 4 片覆盖的字节 [256,320)。
	newData := append([]byte{}, orig...)
	for i := 256; i < 320; i++ {
		newData[i] = 'B'
	}
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), newData, 0o600); err != nil {
		t.Fatal(err)
	}

	// 默认不允许来源变化：必须明确失败，不拼新旧。
	block, _ := NewTask(cfg)
	if _, err := block.Recover(context.Background(), RecoverOptions{}); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("changed source without opt-in must mismatch, got %#v", err)
	}

	// 允许后：仅第 4 片重做，其余复用；结果与用新内容整体重跑完全一致。
	counted := &countingRandomSource{}
	var innerErr error
	counted.RandomFileSource, innerErr = OpenRandomFile("a", filepath.Join(dir, "a.bin"), 2)
	if innerErr != nil {
		t.Fatal(innerErr)
	}
	if err := counted.RandomFileSource.InitFingerprint(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg2 := cfg
	cfg2.newSource = func(spec SourceSpec) (Source, error) {
		if spec.ID == "a" {
			return counted, nil
		}
		return nil, fmt.Errorf("unexpected source %s", spec.ID)
	}
	task, _ := NewTask(cfg2)
	rep, err := task.Recover(context.Background(), RecoverOptions{AllowChangedSources: true})
	if err != nil {
		t.Fatalf("incremental recover: %v", err)
	}
	if !intsEqualSet(rep.Repaired, []int{4}) {
		t.Fatalf("want only shard 4 redone, got %v", rep.Repaired)
	}
	if len(rep.ChangedSources) != 1 || rep.ChangedSources[0] != "a" {
		t.Fatalf("changed sources report wrong: %v", rep.ChangedSources)
	}
	if !bytes.Equal(readFile(t, cfg2.OutputPath), newData) {
		t.Fatal("incremental result != full rerun over new content")
	}
	if n := counted.reads.Load(); n != 1 {
		t.Fatalf("exactly 1 source read expected (only affected shard), got %d", n)
	}
	// 修复后检查点指纹已更新：再次普通 Run/Recover 必须全部复用、不再报变化。
	task2, _ := NewTask(cfg2)
	rep2, err := task2.Recover(context.Background(), RecoverOptions{AllowChangedSources: true})
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if len(rep2.Repaired) != 0 {
		t.Fatalf("second recover must redo nothing, got %v", rep2.Repaired)
	}
	// 普通严格 Run 也应成功（指纹已同步）。
	task3, _ := NewTask(cfg)
	if err := task3.Run(context.Background()); err != nil {
		t.Fatalf("strict run after fingerprint sync: %v", err)
	}
}

func TestRecover_SourceSizeGrowRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)
	// 追加 64 字节 => 大小变化，无法可靠映射布局。
	f, err := os.OpenFile(filepath.Join(dir, "a.bin"), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write(bytes.Repeat([]byte{'B'}, 64))
	_ = f.Close()
	task, _ := NewTask(cfg)
	_, err = task.Recover(context.Background(), RecoverOptions{AllowChangedSources: true})
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindSourceLayoutChanged {
		t.Fatalf("size change must be KindSourceLayoutChanged, got %#v", err)
	}
	if !errors.Is(err, ErrSourceLayoutChanged) {
		t.Fatalf("errors.Is ErrSourceLayoutChanged failed: %#v", err)
	}
}

func TestRecover_SequentialSameSizeChangeRedoesSuffix(t *testing.T) {
	dir := t.TempDir()
	orig := bytes.Repeat([]byte{'S'}, 64*10)
	if err := os.WriteFile(filepath.Join(dir, "s.bin"), orig, 0o600); err != nil {
		t.Fatal(err)
	}
	specs := []SourceSpec{{ID: "s", Path: filepath.Join(dir, "s.bin"), Kind: Sequential}}
	cfg := newCompletedTask(t, dir, specs, 64, 64*10)

	// 同大小改写顺序源的第 2 片 [128,192)：受游标单调约束，第 2 片起全部重做。
	newData := append([]byte{}, orig...)
	for i := 128; i < 192; i++ {
		newData[i] = 'T'
	}
	if err := os.WriteFile(filepath.Join(dir, "s.bin"), newData, 0o600); err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask(cfg)
	rep, err := task.Recover(context.Background(), RecoverOptions{AllowChangedSources: true})
	if err != nil {
		t.Fatalf("sequential incremental recover: %v", err)
	}
	if !intsEqualSet(rep.Repaired, []int{2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("want suffix 2..9 redone, got %v", rep.Repaired)
	}
	if !intsEqualSet(rep.Reused, []int{0, 1}) {
		t.Fatalf("want 0,1 reused, got %v", rep.Reused)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), newData) {
		t.Fatal("sequential incremental result != full rerun over new content")
	}
}

func TestRecover_MultiSourceChangeIsolatesUnaffected(t *testing.T) {
	dir := t.TempDir()
	ra := bytes.Repeat([]byte{'R'}, 640)
	sq := bytes.Repeat([]byte{'Q'}, 320)
	if err := os.WriteFile(filepath.Join(dir, "r.bin"), ra, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "s.bin"), sq, 0o600); err != nil {
		t.Fatal(err)
	}
	specs := []SourceSpec{
		{ID: "r", Path: filepath.Join(dir, "r.bin"), Kind: RandomAccess, MaxConcurrency: 2},
		{ID: "s", Path: filepath.Join(dir, "s.bin"), Kind: Sequential},
	}
	cfg := newCompletedTask(t, dir, specs, 64, 0)

	// 只改随机源 r 的第 1 片（全局 shard 1）；顺序源 s 的全局 shard 10..14 必须全部复用。
	newRA := append([]byte{}, ra...)
	for i := 64; i < 128; i++ {
		newRA[i] = 'Z'
	}
	if err := os.WriteFile(filepath.Join(dir, "r.bin"), newRA, 0o600); err != nil {
		t.Fatal(err)
	}
	task, _ := NewTask(cfg)
	rep, err := task.Recover(context.Background(), RecoverOptions{AllowChangedSources: true})
	if err != nil {
		t.Fatalf("multi-source recover: %v", err)
	}
	if !intsEqualSet(rep.Repaired, []int{1}) {
		t.Fatalf("only global shard 1 must be redone, got %v", rep.Repaired)
	}
	// 顺序源的片必须在复用集合里。
	for idx := 10; idx <= 14; idx++ {
		found := false
		for _, v := range rep.Reused {
			if v == idx {
				found = true
			}
		}
		if !found {
			t.Fatalf("sequential source shard %d must be reused", idx)
		}
	}
	want := append(append([]byte{}, newRA...), sq...)
	if !bytes.Equal(readFile(t, cfg.OutputPath), want) {
		t.Fatal("multi-source incremental result != concatenation of new r + unchanged s")
	}
}

// TestRecover_AfterHardKillSimulated 模拟 kill -9：任务跑到一半，输出与检查点可能不一致，
// 然后不重启任务而是直接体检 + 修复，最终结果必须等于全部重跑。
func TestRecover_AfterHardKillSimulated(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'A', 64*30)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := cfgFor(dir, specs, 64)

	// 跑到约一半取消（进程被杀死的等价物：已确认片落盘，可能有未确认尾部）。
	ctx, cancel := context.WithCancel(context.Background())
	t1, _ := NewTask(cfg)
	runErr := make(chan error, 1)
	go func() { runErr <- t1.Run(ctx) }()
	waitForCompleted(t, cfg.CheckpointPath, 5, 3*time.Second)
	cancel()
	<-runErr

	// 人为制造“已确认片在输出中损坏”（断电写坏的等价物）。
	f, err := os.OpenFile(cfg.OutputPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteAt([]byte("KILL9!!"), 10)
	_ = f.Close()

	// 体检（只读）必须锁定 shard 0。
	it, _ := NewTask(cfg)
	rep0, err := it.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !rep0.Damaged[0] {
		t.Fatalf("shard 0 must be flagged damaged, got %v", rep0.Damaged)
	}

	// 修复：坏片重做 + 上次未完成片补齐。
	rep := runRecover(t, cfg, RecoverOptions{})
	if !sliceContains(rep.Repaired, 0) {
		t.Fatalf("damaged shard 0 must be repaired, got %v", rep.Repaired)
	}
	if len(rep.Repaired)+len(rep.Reused)+len(rep.Pending) != 30 {
		t.Fatalf("report must cover all 30 shards: reused=%d repaired=%d pending=%d",
			len(rep.Reused), len(rep.Repaired), len(rep.Pending))
	}
	if len(rep.Pending) == 0 {
		t.Fatal("shards never confirmed before the kill must be completed as pending")
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("post-kill repair output != full serial rerun")
	}
	// 修复再修复：稳定，不重复处理。
	again := runRecover(t, cfg, RecoverOptions{})
	if len(again.Repaired) != 0 {
		t.Fatalf("repeated repair must redo nothing, got %v", again.Repaired)
	}
}

func sliceContains(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func TestRecover_ThenInspectTwiceStable(t *testing.T) {
	dir := t.TempDir()
	data := writeFile(t, filepath.Join(dir, "a.bin"), 'A', 640)
	specs := []SourceSpec{{ID: "a", Path: filepath.Join(dir, "a.bin"), Kind: RandomAccess, MaxConcurrency: 2}}
	cfg := newCompletedTask(t, dir, specs, 64, 640)
	f, _ := os.OpenFile(cfg.OutputPath, os.O_RDWR, 0o600)
	_, _ = f.WriteAt([]byte("ZZ"), 64)
	_ = f.Close()

	for i := 0; i < 2; i++ {
		runRecover(t, cfg, RecoverOptions{})
		insp, _ := NewTask(cfg)
		rep, err := insp.Inspect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !rep.Healthy() {
			t.Fatalf("iteration %d not healthy: %v", i, rep.Damaged)
		}
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("output drifted over repeated recover/inspect")
	}
}
