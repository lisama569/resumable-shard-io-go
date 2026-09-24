package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
)

// SourceSpec 在任务配置中声明一个本地来源。
type SourceSpec struct {
	ID   string
	Path string
	Kind SourceKind
	// MaxConcurrency 仅对随机访问型有意义；顺序型强制为 1。
	MaxConcurrency int
}

// TaskConfig 描述一次可恢复的分片合并任务。
type TaskConfig struct {
	TaskID         string
	SourceSpecs    []SourceSpec
	OutputPath     string
	CheckpointPath string
	ShardSize      int64
	Pipeline       PipelineConfig
	Logger         *slog.Logger

	// newSource 为可选的来源构造钩子（主要用于测试注入可控来源）；
	// 为 nil 时按 Spec 的 Kind 打开真实本地文件。
	newSource func(spec SourceSpec) (Source, error)
}

// Task 编排：打开来源 → 加载/创建检查点 → 校验兼容 → 回读校验已完成分片 →
// 流水线并发读取 → 按序合并输出 → 最终串行等价校验。
type Task struct {
	cfg     TaskConfig
	log     *slog.Logger
	sources []Source
	states  []SourceState
	shards  []Shard
	cp      *Checkpoint
	out     *os.File
}

// NewTask 创建任务（暂不触碰文件系统）。
func NewTask(cfg TaskConfig) (*Task, error) {
	if cfg.TaskID == "" {
		return nil, newError(KindInvalidArgument, "", "TaskID is required", nil)
	}
	if cfg.ShardSize <= 0 {
		return nil, newError(KindInvalidArgument, "", "ShardSize must be positive", nil)
	}
	if cfg.CheckpointPath == "" {
		return nil, newError(KindInvalidArgument, "", "CheckpointPath is required", nil)
	}
	if cfg.OutputPath == "" {
		return nil, newError(KindInvalidArgument, "", "OutputPath is required", nil)
	}
	if len(cfg.SourceSpecs) == 0 {
		return nil, newError(KindInvalidArgument, "", "at least one source is required", nil)
	}
	ids := map[string]bool{}
	for i, sp := range cfg.SourceSpecs {
		if sp.ID == "" {
			return nil, newError(KindInvalidArgument, "", fmt.Sprintf("source #%d missing id", i), nil)
		}
		if ids[sp.ID] {
			return nil, newError(KindInvalidArgument, sp.ID, "duplicate source id", nil)
		}
		ids[sp.ID] = true
		if sp.Kind != RandomAccess && sp.Kind != Sequential {
			return nil, newError(KindInvalidArgument, sp.ID, "unknown source kind: "+string(sp.Kind), nil)
		}
		if sp.Path == "" {
			return nil, newError(KindInvalidArgument, sp.ID, "empty source path", nil)
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Pipeline.Logger == nil {
		cfg.Pipeline.Logger = cfg.Logger
	}
	return &Task{cfg: cfg, log: cfg.Logger}, nil
}

// openSources 打开全部来源并计算指纹；任一失败（含权限不足）立即整体失败，
// 不进行任何读取（明确的失败隔离：一个来源不可用，整个任务不可启动）。
func (t *Task) openSources(ctx context.Context) error {
	for _, sp := range t.cfg.SourceSpecs {
		var src Source
		if t.cfg.newSource != nil {
			s, err := t.cfg.newSource(sp)
			if err != nil {
				return err
			}
			src = s
		} else {
			switch sp.Kind {
			case RandomAccess:
				max := sp.MaxConcurrency
				if max < 1 {
					max = 1
				}
				rs, err := OpenRandomFile(sp.ID, sp.Path, max)
				if err != nil {
					return err
				}
				if err := rs.InitFingerprint(ctx); err != nil {
					_ = rs.Close()
					return err
				}
				src = rs
			case Sequential:
				ss, err := OpenSequentialFile(sp.ID, sp.Path)
				if err != nil {
					return err
				}
				if err := ss.InitFingerprint(ctx); err != nil {
					_ = ss.Close()
					return err
				}
				src = ss
			}
		}
		t.sources = append(t.sources, src)
		st := SourceState{
			ID:             sp.ID,
			Kind:           sp.Kind,
			MaxConcurrency: src.MaxConcurrency(),
		}
		fp, err := src.Fingerprint(ctx)
		if err != nil {
			return err
		}
		st.Fingerprint = fp
		t.states = append(t.states, st)
	}
	shards, err := PlanShards(t.states, t.cfg.ShardSize)
	if err != nil {
		return err
	}
	t.shards = shards
	return nil
}

func (t *Task) closeSources() {
	for _, s := range t.sources {
		_ = s.Close()
	}
	t.sources = nil
}

// Run 执行（或恢复）任务直至完成。
func (t *Task) Run(ctx context.Context) (retErr error) {
	log := t.log
	if err := t.openSources(ctx); err != nil {
		return err
	}
	defer t.closeSources()

	// 打开输出文件（若已存在必须是普通可读写文件，用于定点恢复）。
	out, err := os.OpenFile(t.cfg.OutputPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		if os.IsPermission(err) {
			return newError(KindPermission, "", "output open not permitted: "+t.cfg.OutputPath, err)
		}
		return newError(KindSourceUnreadable, "", "output open failed: "+t.cfg.OutputPath, err)
	}
	t.out = out
	defer func() { _ = out.Close() }()

	// ---- 加载或创建检查点 ----
	cp, err := LoadCheckpoint(t.cfg.CheckpointPath)
	if err != nil {
		return err // 已分类：corrupt / version / permission
	}
	resuming := cp != nil
	if !resuming {
		cp = &Checkpoint{
			Version:   checkpointVersion,
			TaskID:    t.cfg.TaskID,
			Output:    t.cfg.OutputPath,
			ShardSize: t.cfg.ShardSize,
			Sources:   t.states,
			Completed: map[int]CompletedShard{},
		}
		// 先写一个空检查点，确认检查点路径可写，避免读到一半才发现无法持久化。
		if err := cp.Save(t.cfg.CheckpointPath); err != nil {
			return err
		}
		log.InfoContext(ctx, "checkpoint created",
			slog.String("task", t.cfg.TaskID),
			slog.Int("checkpoint_version", checkpointVersion),
			slog.Int("shards", len(t.shards)),
			slog.String("basis", "no existing checkpoint"))
	} else {
		log.InfoContext(ctx, "checkpoint loaded",
			slog.String("task", t.cfg.TaskID),
			slog.Int("checkpoint_version", cp.Version),
			slog.Int("completed", len(cp.Completed)),
			slog.String("basis", "resuming"))
		// 严格兼容性校验：任务标识/输出/分片大小/来源声明/任一来源指纹不匹配均明确失败。
		if err := cp.ValidateAgainst(t.cfg.TaskID, t.cfg.OutputPath, t.cfg.ShardSize, t.states); err != nil {
			return err
		}
		// 检查点自身的完成记录必须与当前分片计划（来源大小派生）对得上。
		if err := t.validateCompletedLayout(cp); err != nil {
			return err
		}
		// 恢复前回读输出文件中的已确认分片，防止输出被外部删改却被误认为成功。
		if err := t.verifyCompletedOutput(ctx, cp); err != nil {
			return err
		}
		// 将顺序源游标推进到各来源已确认的连续前缀末尾。
		if err := t.resumeSequentialCursors(cp); err != nil {
			return err
		}
	}
	t.cp = cp

	// 若全部完成（纯恢复场景），直接做最终校验。
	if len(cp.Completed) < len(t.shards) {
		if err := t.executePipeline(ctx, cp, resuming); err != nil {
			return err
		}
	}

	// ---- 最终校验：输出文件必须与“按来源顺序、按偏移串行读取”的拼接逐字节等价 ----
	finalHash, err := t.finalVerify(ctx, cp)
	if err != nil {
		return err
	}
	cp.FinalHash = finalHash
	if err := cp.Save(t.cfg.CheckpointPath); err != nil {
		return err
	}
	log.InfoContext(ctx, "task completed",
		slog.String("task", t.cfg.TaskID),
		slog.Int("shards", len(t.shards)),
		slog.String("final_sha256", cp.FinalHash),
		slog.String("basis", "serial-equivalent merge verified"))
	return nil
}

// validateCompletedLayout 确认完成记录与当前分片计划一致（索引、来源、区间、长度）。
func (t *Task) validateCompletedLayout(cp *Checkpoint) error {
	for idx, cs := range cp.Completed {
		if idx < 0 || idx >= len(t.shards) {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("completed shard index %d outside current plan of %d shards", idx, len(t.shards)),
				ErrCheckpointMismatch)
		}
		sh := t.shards[idx]
		if sh.SourceID != cs.SourceID || sh.Start != cs.Start || sh.End != cs.End {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("completed shard %d layout {source=%s [%d,%d)} != plan {source=%s [%d,%d)}",
					idx, cs.SourceID, cs.Start, cs.End, sh.SourceID, sh.Start, sh.End),
				ErrCheckpointMismatch)
		}
		if int64(cs.Bytes) != sh.Length() {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("completed shard %d bytes %d != plan length %d", idx, cs.Bytes, sh.Length()),
				ErrCheckpointMismatch)
		}
	}
	return nil
}

// globalOffset 返回分片在合并输出中的起始偏移。
func (t *Task) globalOffset(index int) int64 {
	var off int64
	for i := 0; i < index; i++ {
		off += t.shards[i].Length()
	}
	return off
}

// verifyCompletedOutput 回读输出文件中已确认区间并复核长度与 SHA-256。
func (t *Task) verifyCompletedOutput(ctx context.Context, cp *Checkpoint) error {
	info, err := t.out.Stat()
	if err != nil {
		return newError(KindSourceUnreadable, "", "stat output failed during resume verify", err)
	}
	indexes := make([]int, 0, len(cp.Completed))
	for idx := range cp.Completed {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	for _, idx := range indexes {
		cs := cp.Completed[idx]
		off := t.globalOffset(idx)
		if off+int64(cs.Bytes) > info.Size() {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("output too small for completed shard %d: need [%d,%d), file size=%d (output file replaced/truncated?)",
					idx, off, off+int64(cs.Bytes), info.Size()), ErrCheckpointMismatch)
		}
		buf := make([]byte, cs.Bytes)
		if _, err := readFullAt(ctx, t.out, buf, off); err != nil {
			return newError(KindSourceUnreadable, cs.SourceID,
				fmt.Sprintf("re-read completed shard %d failed", idx), err)
		}
		if got := checksumBytes(buf); got != cs.Checksum {
			return newError(KindChecksumMismatch, cs.SourceID,
				fmt.Sprintf("resume verify failed for shard %d [%d,%d): recorded=%s output=%s",
					idx, cs.Start, cs.End, cs.Checksum, got), ErrChecksumMismatch)
		}
		t.log.DebugContext(ctx, "resume verify ok",
			slog.Int("index", idx), slog.String("source", cs.SourceID),
			slog.Int64("start", cs.Start), slog.Int64("end", cs.End),
			slog.Int64("output_offset", off), slog.String("checksum", cs.Checksum))
	}
	return nil
}

// resumeSequentialCursors 把顺序源游标推进到已确认的连续前缀末尾。
// 顺序源单调派发保证每个顺序源的已完成索引集合在其自身分片序列上为连续前缀。
func (t *Task) resumeSequentialCursors(cp *Checkpoint) error {
	// 按来源聚合其分片序列。
	bySource := map[string][]Shard{}
	for _, sh := range t.shards {
		bySource[sh.SourceID] = append(bySource[sh.SourceID], sh)
	}
	for _, src := range t.sources {
		if src.Kind() != Sequential {
			continue
		}
		var advance int64
		for _, sh := range bySource[src.ID()] {
			if !cp.IsCompleted(sh.Index) {
				break
			}
			advance = sh.End
		}
		if ss, ok := src.(*SequentialFileSource); ok && advance > 0 {
			if err := ss.ResumeAt(advance); err != nil {
				return err
			}
			t.log.InfoContext(context.Background(), "sequential source resumed",
				slog.String("source", src.ID()), slog.Int64("cursor", advance),
				slog.String("basis", "contiguous confirmed prefix skipped"))
		}
	}
	return nil
}

// executePipeline 运行流水线并把按序结果落盘 + 逐片确认检查点。
func (t *Task) executePipeline(ctx context.Context, cp *Checkpoint, resuming bool) error {
	srcMap := make(map[string]Source, len(t.sources))
	for _, s := range t.sources {
		srcMap[s.ID()] = s
	}
	done := cp.SnapshotCompleted()

	pipe := NewPipeline(t.cfg.Pipeline)
	results := make(chan ShardResult)
	pipeErr := make(chan error, 1)
	go func() {
		pipeErr <- pipe.Run(ctx, srcMap, t.shards, done, results)
	}()

	for res := range results {
		// 结果按序到达：写入输出文件的对应全局偏移。
		off := t.globalOffset(res.Shard.Index)
		if err := writeFullAt(ctx, t.out, res.Data, off); err != nil {
			// 排空 results 后返回，保证 Run 正常收尾（无 goroutine 泄漏）。
			go func() {
				for range results {
				}
			}()
			<-pipeErr
			return err
		}
		// 片内校验：长度与读取前计划一致；校验和与数据自洽（已在 worker 计算）。
		if int64(len(res.Data)) != res.Shard.Length() {
			return newError(KindChecksumMismatch, res.Shard.SourceID,
				fmt.Sprintf("shard %d read %d bytes, want %d", res.Shard.Index, len(res.Data), res.Shard.Length()),
				ErrChecksumMismatch)
		}
		cs := CompletedShard{
			Index:    res.Shard.Index,
			SourceID: res.Shard.SourceID,
			Start:    res.Shard.Start,
			End:      res.Shard.End,
			Checksum: res.Checksum,
			Bytes:    len(res.Data),
		}
		if err := cp.MarkCompleted(cs); err != nil {
			go func() {
				for range results {
				}
			}()
			<-pipeErr
			return err
		}
		if err := cp.Save(t.cfg.CheckpointPath); err != nil {
			go func() {
				for range results {
				}
			}()
			<-pipeErr
			return err
		}
		t.log.InfoContext(ctx, "shard confirmed",
			slog.Int("index", cs.Index), slog.String("source", cs.SourceID),
			slog.Int64("range_start", cs.Start), slog.Int64("range_end", cs.End),
			slog.Int("bytes", cs.Bytes), slog.String("checksum", cs.Checksum),
			slog.Int("checkpoint_version", cp.Version),
			slog.String("basis", "read+written+checkpointed"))
	}
	if err := <-pipeErr; err != nil {
		return err
	}
	if len(cp.Completed) != len(t.shards) {
		return newError(KindCheckpointCorrupt, "",
			fmt.Sprintf("internal: %d/%d shards completed after pipeline", len(cp.Completed), len(t.shards)), nil)
	}
	return nil
}

// finalVerify 流式重算输出文件的全量 SHA-256（即串行拼接的哈希），并校验：
//   - 输出总大小等于计划的串行拼接总大小；
//   - 逐片回读与检查点记录的每片校验和一致（按计划边界）；
//   - 收尾时来源指纹与开始时一致（读取全程来源未变化）。
//
// 成功时返回输出内容的 SHA-256。
func (t *Task) finalVerify(ctx context.Context, cp *Checkpoint) (string, error) {
	info, err := t.out.Stat()
	if err != nil {
		return "", newError(KindSourceUnreadable, "", "stat output failed in final verify", err)
	}
	var wantSize int64
	for _, sh := range t.shards {
		wantSize += sh.Length()
	}
	if info.Size() != wantSize {
		return "", newError(KindChecksumMismatch, "",
			fmt.Sprintf("output size %d != planned serial-equivalent size %d", info.Size(), wantSize),
			ErrChecksumMismatch)
	}
	// 全量流式读取整个输出并计算 SHA-256，与 MergeSerial 产出的字节序列完全一致。
	if _, err := t.out.Seek(0, io.SeekStart); err != nil {
		return "", newError(KindSourceUnreadable, "", "seek output for final verify failed", err)
	}
	h := sha256.New()
	if _, err := copyWithContext(ctx, h, t.out); err != nil {
		return "", newError(KindSourceUnreadable, "", "final hash read failed", err)
	}
	finalHash := hex.EncodeToString(h.Sum(nil))
	// 逐片回读校验（检查点里每片的 SHA-256）。
	if err := t.verifyCompletedOutput(ctx, cp); err != nil {
		return "", err
	}
	// 复核来源在处理全程未发生变化（收尾指纹必须与开始时一致）。
	for _, want := range t.states {
		var src Source
		for _, s := range t.sources {
			if s.ID() == want.ID {
				src = s
			}
		}
		fp, err := src.Fingerprint(ctx)
		if err != nil {
			return "", err
		}
		if fp != want.Fingerprint {
			return "", newError(KindSourceChanged, want.ID,
				fmt.Sprintf("source changed during task: start={size=%d hash=%s} end={size=%d hash=%s}",
					want.Fingerprint.Size, want.Fingerprint.Hash, fp.Size, fp.Hash), nil)
		}
	}
	return finalHash, nil
}

// readFullAt 从 f 的 off 处读满 len(buf)，期间响应取消。
func readFullAt(ctx context.Context, f *os.File, buf []byte, off int64) (int, error) {
	var n int
	for n < len(buf) {
		if err := ctx.Err(); err != nil {
			return n, classifyCtxErr(ctx, err)
		}
		nr, err := f.ReadAt(buf[n:], off+int64(n))
		n += nr
		if err != nil {
			if err == io.EOF && n == len(buf) {
				return n, nil
			}
			return n, err
		}
	}
	return n, nil
}

// writeFullAt 向 f 的 off 处写完整 buf，期间响应取消。
func writeFullAt(ctx context.Context, f *os.File, buf []byte, off int64) error {
	var n int
	for n < len(buf) {
		if err := ctx.Err(); err != nil {
			return classifyCtxErr(ctx, err)
		}
		nw, err := f.WriteAt(buf[n:], off+int64(n))
		n += nw
		if err != nil {
			if os.IsPermission(err) {
				return newError(KindPermission, "", "output write not permitted", err)
			}
			return newError(KindSourceUnreadable, "", "output write failed", err)
		}
	}
	return nil
}

// Close 释放任务资源。
func (t *Task) Close() error {
	if t.out != nil {
		err := t.out.Close()
		t.out = nil
		return err
	}
	return nil
}

// MergeSerial 是参考实现：按来源顺序、按偏移串行读取全部数据并写入 w。
// 其输出定义了流水线合并结果必须满足的“串行等价”标准，测试与文档引用它。
func MergeSerial(ctx context.Context, sources []Source, states []SourceState, shardSize int64, w io.Writer) (string, error) {
	shards, err := PlanShards(states, shardSize)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	mw := io.MultiWriter(w, h)
	for _, sh := range shards {
		var src Source
		for _, s := range sources {
			if s.ID() == sh.SourceID {
				src = s
			}
		}
		data, err := src.ReadShard(ctx, sh.Start, sh.End)
		if err != nil {
			return "", err
		}
		if _, err := mw.Write(data); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
