package shardio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// doctorCfg 构造一个确定性的单随机源任务配置（每片 64B，共 nShards 片）。
func doctorCfg(t *testing.T, dir string, nShards int, fill byte) (TaskConfig, []byte) {
	t.Helper()
	data := bytes.Repeat([]byte{fill}, 64*nShards)
	path := filepath.Join(dir, "a.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := TaskConfig{
		TaskID:         "doctor-" + filepath.Base(dir),
		SourceSpecs:    []SourceSpec{{ID: "a", Path: path, Kind: RandomAccess, MaxConcurrency: 2}},
		OutputPath:     filepath.Join(dir, "out.bin"),
		CheckpointPath: filepath.Join(dir, "ckpt.json"),
		ShardSize:      64,
		Pipeline:       PipelineConfig{GlobalWorkers: 3, InFlightBytes: 400},
		Logger:         quietLogger(),
	}
	return cfg, data
}

func runToCompletion(t *testing.T, cfg TaskConfig) {
	t.Helper()
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
}

// countSource 包装一个真实随机源，统计 ReadShard 调用次数（用于证明“复用片不重读”）。
type countSource struct {
	Source
	reads atomic.Int64
}

func (c *countSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	c.reads.Add(1)
	return c.Source.ReadShard(ctx, start, end)
}

// 场景 1：kill -9 后输出被损坏 —— 体检只读、定位坏片，修复只重做坏片且与全量重跑一致；
// 体检与修复各自重复执行均稳定幂等。
func TestDoctor_ForceKill_ThenDiagnoseAndSelfHeal(t *testing.T) {
	dir := t.TempDir()
	cfg, data := doctorCfg(t, dir, 20, 'A')
	runToCompletion(t, cfg)

	// 模拟断电后输出中第 3、7 片所在区间被破坏（检查点仍称这些片已完成）。
	outRaw, err := os.ReadFile(cfg.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte{}, outRaw...)
	corrupt[3*64+10] = 'X'
	corrupt[3*64+11] = 'Y'
	corrupt[7*64+5] = 'Z'
	if err := os.WriteFile(cfg.OutputPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	// ---- 第一次体检：只读，必须定位两片 output_damaged，其余 ok ----
	beforeCKPT := readFile(t, cfg.CheckpointPath)
	beforeOut := readFile(t, cfg.OutputPath)
	d1, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := d1.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	if rep.TotalCompleted != 20 {
		t.Fatalf("want 20 completed diagnosed, got %d", rep.TotalCompleted)
	}
	if got := rep.ByStatus[StatusOutputDamaged]; got != 2 {
		t.Fatalf("want 2 output_damaged, got %d (%+v)", got, rep.ByStatus)
	}
	if got := rep.ByStatus[StatusOK]; got != 18 {
		t.Fatalf("want 18 ok, got %d", got)
	}
	if got := rep.ByStatus[StatusSourceChanged] + rep.ByStatus[StatusIndeterminable]; got != 0 {
		t.Fatalf("want no source-changed/indeterminable, got %d", got)
	}
	if !rep.Repairable {
		t.Fatal("report must be repairable")
	}
	gotDamaged := map[int]bool{}
	for _, d := range rep.Shards {
		if d.Status == StatusOutputDamaged {
			gotDamaged[d.Index] = true
			if d.OutputHash == d.Recorded || d.CurrentHash != d.Recorded {
				t.Fatalf("shard %d basis wrong: recorded=%s output=%s current=%s",
					d.Index, d.Recorded, d.OutputHash, d.CurrentHash)
			}
		}
	}
	if !gotDamaged[3] || !gotDamaged[7] {
		t.Fatalf("want damaged {3,7}, got %v", gotDamaged)
	}
	// 只读保证：输出与检查点文件内容逐字节不变。
	if !bytes.Equal(readFile(t, cfg.CheckpointPath), beforeCKPT) {
		t.Fatal("doctor modified the checkpoint file")
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), beforeOut) {
		t.Fatal("doctor modified the output file")
	}

	// ---- 体检重复执行：结论稳定 ----
	d2, _ := NewTask(cfg)
	rep2, err := d2.Doctor(context.Background())
	if err != nil {
		t.Fatalf("second doctor: %v", err)
	}
	if rep2.ByStatus[StatusOutputDamaged] != 2 || !rep2.Repairable {
		t.Fatalf("repeated doctor unstable: %+v", rep2.ByStatus)
	}
	if !bytes.Equal(readFile(t, cfg.CheckpointPath), beforeCKPT) {
		t.Fatal("second doctor modified checkpoint")
	}

	// ---- 修复：只重做坏片，最终结果与全量数据一致 ----
	r1, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rrep, err := r1.Repair(context.Background())
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if len(rrep.Repaired) != 2 || rrep.Repaired[0] != 3 || rrep.Repaired[1] != 7 {
		t.Fatalf("want repaired [3 7], got %v", rrep.Repaired)
	}
	if len(rrep.Reused) != 18 {
		t.Fatalf("want 18 reused, got %d", len(rrep.Reused))
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("repaired output != full serial content")
	}
	sum := sha256.Sum256(data)
	if rrep.FinalHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("final hash %s != sha256(data) %s", rrep.FinalHash, hex.EncodeToString(sum[:]))
	}
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	if cp.FinalHash != rrep.FinalHash || len(cp.Completed) != 20 {
		t.Fatal("checkpoint not restored to completed state with final hash")
	}

	// ---- 修复重复执行：幂等空操作，不重复处理，不改坏好片 ----
	r2, _ := NewTask(cfg)
	rrep2, err := r2.Repair(context.Background())
	if err != nil {
		t.Fatalf("second repair: %v", err)
	}
	if rrep2.Rejected {
		t.Fatal("healthy repair must not be rejected")
	}
	if len(rrep2.Repaired) != 0 {
		t.Fatalf("idempotent repair must redo 0 shards, got %v", rrep2.Repaired)
	}
	if len(rrep2.Reused) != 20 {
		t.Fatalf("want 20 reused on repeat repair, got %d", len(rrep2.Reused))
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("repeat repair changed a healthy output")
	}
	// 体检在修复后也应全绿。
	d3, _ := NewTask(cfg)
	rep3, err := d3.Doctor(context.Background())
	if err != nil {
		t.Fatalf("post-repair doctor: %v", err)
	}
	if rep3.ByStatus[StatusOK] != 20 || !rep3.Repairable {
		t.Fatalf("post-repair doctor not all ok: %+v", rep3.ByStatus)
	}
}

// 场景 2：输出文件被整体删除 —— 全部已确认片判 output_damaged，修复等于全量重做且收敛。
func TestDoctor_OutputDeleted_FullRepairIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg, data := doctorCfg(t, dir, 8, 'B')
	runToCompletion(t, cfg)
	if err := os.Remove(cfg.OutputPath); err != nil {
		t.Fatal(err)
	}
	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor with missing output: %v", err)
	}
	if rep.ByStatus[StatusOutputDamaged] != 8 || !rep.Repairable {
		t.Fatalf("want 8 output_damaged+repairable, got %+v", rep.ByStatus)
	}
	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("repair after delete: %v", err)
	}
	if len(rrep.Repaired) != 8 {
		t.Fatalf("want all 8 redone, got %v", rrep.Repaired)
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), data) {
		t.Fatal("output not fully rebuilt")
	}
}

// 场景 3：修复只重读坏片——好片在修复流水线中不被重新读取（复用而非重做）。
func TestDoctor_RepairReusesGoodShardsWithoutReReading(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := doctorCfg(t, dir, 12, 'C')
	var counter countSource
	cfg.newSource = func(spec SourceSpec) (Source, error) {
		inner, err := OpenRandomFile(spec.ID, spec.Path, 2)
		if err != nil {
			return nil, err
		}
		if err := inner.InitFingerprint(context.Background()); err != nil {
			return nil, err
		}
		counter.Source = inner
		return &counter, nil
	}
	runToCompletion(t, cfg) // 首次运行读取 12 次
	firstRun := counter.reads.Load()
	if firstRun != 12 {
		t.Fatalf("baseline reads = %d, want 12", firstRun)
	}

	// 只损坏第 10 片。
	raw := readFile(t, cfg.OutputPath)
	raw[10*64] = 'Q'
	if err := os.WriteFile(cfg.OutputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// 体检会重读来源一次（逐片审计）；修复流水线本身不得重读好片。
	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if len(rrep.Repaired) != 1 || rrep.Repaired[0] != 10 {
		t.Fatalf("want only shard 10 redone, got %v", rrep.Repaired)
	}
	// 修复流水线阶段：只有被撤销的 1 片进入 done=false；读取次数应只 +1
	// （体检阶段的读取在另一个 Task 实例，不计入本包装器——这里复用同一包装器，
	// 故体检也会计数：doctor 审计读 12 片 + 修复流水线读 1 片 = 13）。
	if got := counter.reads.Load() - firstRun; got != 12+1 {
		t.Fatalf("repair phase reads delta=%d, want 13 (12 audit + 1 redo)", got)
	}
}

// overwriteSameSize 在保持文件大小与 mtime 不变的前提下改写内容区间
// （模拟“同大小内容变化”：只能靠全量哈希识别）。
func overwriteSameSize(t *testing.T, path string, at ...int) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range at {
		raw[i] = 'N' // new content marker
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
}

// 场景 4：来源“同大小、部分区间内容变化” —— 只有受影响分片被判 source_changed
// 并重做，未受影响分片复用；修复结果与“以新来源整体重跑”逐字节一致，且重复修复幂等。
func TestDoctor_SameSizePartialChange_IncrementalRerun(t *testing.T) {
	dir := t.TempDir()
	cfg, oldData := doctorCfg(t, dir, 20, 'A')
	runToCompletion(t, cfg)

	// 改动落在第 4 片与第 15 片内（各两字节），其余 18 片内容不变。
	overwriteSameSize(t, cfg.SourceSpecs[0].Path, 4*64+1, 4*64+2, 15*64+63)
	newData, err := os.ReadFile(cfg.SourceSpecs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(newData, oldData) {
		t.Fatal("test setup: source was not changed")
	}

	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor on changed source: %v", err)
	}
	if got := rep.ByStatus[StatusSourceChanged]; got != 2 {
		t.Fatalf("want 2 source_changed, got %d (%+v)", got, rep.ByStatus)
	}
	if got := rep.ByStatus[StatusOK]; got != 18 {
		t.Fatalf("want 18 ok (unaffected reused), got %d", got)
	}
	if !rep.Repairable {
		t.Fatal("same-size partial change must be repairable")
	}
	if len(rep.ChangedSources) != 1 || rep.ChangedSources[0] != "a" {
		t.Fatalf("changed sources = %v, want [a]", rep.ChangedSources)
	}

	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("incremental repair: %v", err)
	}
	if len(rrep.Repaired) != 2 || rrep.Repaired[0] != 4 || rrep.Repaired[1] != 15 {
		t.Fatalf("want repaired [4 15], got %v", rrep.Repaired)
	}
	if len(rrep.Reused) != 18 {
		t.Fatalf("want 18 reused, got %d", len(rrep.Reused))
	}
	if got := readFile(t, cfg.OutputPath); !bytes.Equal(got, newData) {
		t.Fatal("repaired output != full rerun over the NEW source")
	}
	// 修复后检查点的来源指纹必须已推进为新内容指纹（再次 Run/Doctor 全绿）。
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	if cp.Sources[0].Fingerprint.Hash != sha256Hex(newData) {
		t.Fatal("checkpoint source fingerprint not advanced to new content")
	}
	d2, _ := NewTask(cfg)
	rep2, err := d2.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor after incremental repair: %v", err)
	}
	if rep2.ByStatus[StatusOK] != 20 {
		t.Fatalf("post-repair diagnosis not all ok: %+v", rep2.ByStatus)
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// 场景 5：来源大小变化（追加）——分片布局改变，影响范围无法逐片证实：
// 体检给出 indeterminable，Repair 必须明确拒绝，且拒绝不改写任何文件。
func TestDoctor_SourceSizeChange_RepairRejected(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := doctorCfg(t, dir, 20, 'A')
	runToCompletion(t, cfg)
	ckptBefore := readFile(t, cfg.CheckpointPath)
	outBefore := readFile(t, cfg.OutputPath)

	// 追加 3 个分片的字节：分片数量与后续全局偏移全部变化。
	f, err := os.OpenFile(cfg.SourceSpecs[0].Path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{'A'}, 64*3)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor should report indeterminable, not hard-fail: %v", err)
	}
	if rep.Repairable {
		t.Fatal("size-changing source must not be repairable")
	}
	if rep.ByStatus[StatusIndeterminable] == 0 {
		t.Fatalf("want indeterminable shards, got %+v", rep.ByStatus)
	}

	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if !kindIs(err, KindRepairNotPossible) {
		t.Fatalf("want KindRepairNotPossible, got %#v", err)
	}
	if rrep == nil || !rrep.Rejected {
		t.Fatal("repair report must mark Rejected")
	}
	if len(rrep.Repaired) != 0 {
		t.Fatalf("rejected repair must redo nothing, got %v", rrep.Repaired)
	}
	// 拒绝路径不得改动输出与检查点。
	if !bytes.Equal(readFile(t, cfg.CheckpointPath), ckptBefore) {
		t.Fatal("rejected repair modified checkpoint")
	}
	if !bytes.Equal(readFile(t, cfg.OutputPath), outBefore) {
		t.Fatal("rejected repair modified output")
	}
	// 拒绝必须可重复：第二次 Repair 同样稳定拒绝。
	r2, _ := NewTask(cfg)
	rrep2, err2 := r2.Repair(context.Background())
	if !kindIs(err2, KindRepairNotPossible) || !rrep2.Rejected {
		t.Fatalf("repeated rejected repair unstable: %#v", err2)
	}
}

// 场景 6：多来源中仅一个“同大小”变化 —— 只重做该来源的受影响分片，
// 其他来源的已确认分片继续复用；最终等于按新来源整体重跑。
func TestDoctor_MultiSource_OnlyAffectedSourceRedone(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.bin")
	pathB := filepath.Join(dir, "b.bin")
	dataA0 := bytes.Repeat([]byte{'A'}, 64*10)
	dataB := bytes.Repeat([]byte{'B'}, 64*6)
	if err := os.WriteFile(pathA, dataA0, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathB, dataB, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := TaskConfig{
		TaskID: "doctor-multi",
		SourceSpecs: []SourceSpec{
			{ID: "a", Path: pathA, Kind: RandomAccess, MaxConcurrency: 2},
			{ID: "b", Path: pathB, Kind: Sequential},
		},
		OutputPath:     filepath.Join(dir, "out.bin"),
		CheckpointPath: filepath.Join(dir, "ckpt.json"),
		ShardSize:      64,
		Pipeline:       PipelineConfig{GlobalWorkers: 3, InFlightBytes: 400},
		Logger:         quietLogger(),
	}
	runToCompletion(t, cfg)

	// 只改来源 a 的第 2 片；来源 b 完全不变（顺序源，分片索引 10..15）。
	overwriteSameSize(t, pathA, 2*64+7)
	dataA1, _ := os.ReadFile(pathA)

	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	if rep.ByStatus[StatusSourceChanged] != 1 {
		t.Fatalf("want exactly 1 source_changed shard, got %+v", rep.ByStatus)
	}
	var changedIdx int
	for _, sd := range rep.Shards {
		if sd.Status == StatusSourceChanged {
			changedIdx = sd.Index
		}
		if sd.SourceID == "b" && sd.Status != StatusOK {
			t.Fatalf("unchanged sequential source shard %d must be ok, got %s", sd.Index, sd.Status)
		}
	}
	if changedIdx != 2 {
		t.Fatalf("want changed shard index 2, got %d", changedIdx)
	}
	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if len(rrep.Repaired) != 1 || rrep.Repaired[0] != 2 {
		t.Fatalf("want only shard 2 redone, got %v", rrep.Repaired)
	}
	want := append(append([]byte{}, dataA1...), dataB...)
	if got := readFile(t, cfg.OutputPath); !bytes.Equal(got, want) {
		t.Fatal("multi-source incremental repair != full rerun with new a + unchanged b")
	}
}

// 场景 7：顺序型来源的早期分片损坏 —— 后继分片必须连带重做（游标不能跳跃），
// 但同任务中的其他来源不受牵连。
func TestDoctor_SequentialSource_SuffixRedo(t *testing.T) {
	dir := t.TempDir()
	pathS := filepath.Join(dir, "s.bin")
	pathR := filepath.Join(dir, "r.bin")
	dataS := bytes.Repeat([]byte{'S'}, 64*10)
	dataR := bytes.Repeat([]byte{'R'}, 64*4)
	if err := os.WriteFile(pathS, dataS, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pathR, dataR, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := TaskConfig{
		TaskID: "doctor-seq",
		SourceSpecs: []SourceSpec{
			{ID: "s", Path: pathS, Kind: Sequential},
			{ID: "r", Path: pathR, Kind: RandomAccess, MaxConcurrency: 2},
		},
		OutputPath:     filepath.Join(dir, "out.bin"),
		CheckpointPath: filepath.Join(dir, "ckpt.json"),
		ShardSize:      64,
		Pipeline:       PipelineConfig{GlobalWorkers: 3, InFlightBytes: 400},
		Logger:         quietLogger(),
	}
	runToCompletion(t, cfg)

	// 损坏顺序源 s 的第 1 片（全局索引 1；s 的索引区间是 0..9，r 是 10..13）。
	raw := readFile(t, cfg.OutputPath)
	raw[64+3] = 'X'
	if err := os.WriteFile(cfg.OutputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor: %v", err)
	}
	if rep.ByStatus[StatusOutputDamaged] != 1 {
		t.Fatalf("want 1 output_damaged at diagnosis, got %+v", rep.ByStatus)
	}
	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("sequential suffix repair: %v", err)
	}
	// 体检只报 1 片损坏，但修复重做集合必须扩展为 s 的后缀 1..9（共 9 片）。
	if len(rrep.Repaired) != 9 {
		t.Fatalf("want suffix shards [1..9] redone (9), got %v", rrep.Repaired)
	}
	for i, idx := range rrep.Repaired {
		if idx != i+1 {
			t.Fatalf("redone set not the sequential suffix 1..9: %v", rrep.Repaired)
		}
	}
	// 随机源 r 的 4 片（10..13）继续复用。
	for _, idx := range []int{10, 11, 12, 13} {
		if containsInt(rrep.Repaired, idx) {
			t.Fatalf("random source shard %d must be reused", idx)
		}
	}
	want := append(append([]byte{}, dataS...), dataR...)
	if got := readFile(t, cfg.OutputPath); !bytes.Equal(got, want) {
		t.Fatal("sequential suffix repair output mismatch")
	}
	// 再次修复必须全量复用、幂等。
	r2, _ := NewTask(cfg)
	rrep2, err := r2.Repair(context.Background())
	if err != nil || len(rrep2.Repaired) != 0 {
		t.Fatalf("repeat sequential repair not idempotent: %v redone=%v", err, rrep2.Repaired)
	}
}

// 场景 12：任务只确认了部分分片即被强杀，且其中一片输出损坏 —— Repair 既要
// 重做坏片，也要续跑从未完成的分片；修复后再跑一次为幂等空操作，结果同全量重跑。
func TestDoctor_PartialProgressKill_DamageRepairAndResume(t *testing.T) {
	dir := t.TempDir()
	cfg, data := doctorCfg(t, dir, 24, 'A')

	// 跑到确认 8 片后取消（模拟强杀留下的部分检查点）。
	ctx, cancel := context.WithCancel(context.Background())
	t1, _ := NewTask(cfg)
	runErr := make(chan error, 1)
	go func() { runErr <- t1.Run(ctx) }()
	waitForCompleted(t, cfg.CheckpointPath, 8, 3*time.Second)
	cancel()
	<-runErr
	cp, _ := LoadCheckpoint(cfg.CheckpointPath)
	if len(cp.Completed) < 8 {
		t.Fatalf("want >=8 confirmed, got %d", len(cp.Completed))
	}

	// 破坏一个已确认分片（第 2 片）。
	raw := readFile(t, cfg.OutputPath)
	if int64(len(raw)) < 3*64 {
		t.Fatalf("partial output unexpectedly short: %d", len(raw))
	}
	raw[2*64] = '!'
	if err := os.WriteFile(cfg.OutputPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	d, _ := NewTask(cfg)
	rep, err := d.Doctor(context.Background())
	if err != nil {
		t.Fatalf("doctor on partial run: %v", err)
	}
	if rep.ByStatus[StatusOutputDamaged] != 1 {
		t.Fatalf("want 1 damaged, got %+v", rep.ByStatus)
	}
	if rep.TotalCompleted != len(cp.Completed) {
		t.Fatalf("doctor must diagnose only completed shards, got %d want %d",
			rep.TotalCompleted, len(cp.Completed))
	}

	r, _ := NewTask(cfg)
	rrep, err := r.Repair(context.Background())
	if err != nil {
		t.Fatalf("repair partial: %v", err)
	}
	if !containsInt(rrep.Repaired, 2) {
		t.Fatalf("damaged shard 2 must be redone, got %v", rrep.Repaired)
	}
	if got := readFile(t, cfg.OutputPath); !bytes.Equal(got, data) {
		t.Fatal("partial-kill repair output != full rerun")
	}
	cp2, _ := LoadCheckpoint(cfg.CheckpointPath)
	if len(cp2.Completed) != 24 || cp2.FinalHash == "" {
		t.Fatalf("repair must finish all 24 with final hash, got %d completed", len(cp2.Completed))
	}

	// 再来一次：无坏片、无缺片 -> 幂等空操作。
	r2, _ := NewTask(cfg)
	rrep2, err := r2.Repair(context.Background())
	if err != nil || len(rrep2.Repaired) != 0 || len(rrep2.Reused) != 24 {
		t.Fatalf("repeat repair not idempotent: err=%v redone=%v reused=%d",
			err, rrep2.Repaired, len(rrep2.Reused))
	}
	if got := readFile(t, cfg.OutputPath); !bytes.Equal(got, data) {
		t.Fatal("idempotent re-repair altered output")
	}
}

// 场景 13：同一 Task 实例上连续调用 Doctor/Repair —— 每次都重新打开并关闭
// 真实来源句柄，不泄漏文件描述符、不互相干扰。
func TestDoctor_SameTaskInstanceRepeatedOpensAndClosesSources(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := doctorCfg(t, dir, 6, 'A')
	runToCompletion(t, cfg)

	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		rep, err := task.Doctor(context.Background())
		if err != nil {
			t.Fatalf("doctor iteration %d: %v", i, err)
		}
		if rep.ByStatus[StatusOK] != 6 {
			t.Fatalf("iteration %d: want 6 ok, got %+v", i, rep.ByStatus)
		}
		if task.sources != nil {
			t.Fatalf("iteration %d: sources not closed after Doctor", i)
		}
	}
	for i := 0; i < 2; i++ {
		rrep, err := task.Repair(context.Background())
		if err != nil {
			t.Fatalf("repair iteration %d: %v", i, err)
		}
		if len(rrep.Repaired) != 0 {
			t.Fatalf("iteration %d: want idempotent no-op, redone=%v", i, rrep.Repaired)
		}
		if task.sources != nil {
			t.Fatalf("iteration %d: sources not closed after Repair", i)
		}
	}
}
