package shardio

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
)

// ShardVerdict 是体检对单个已确认分片给出的判定结论。
type ShardVerdict string

const (
	// ShardOK：输出中该片实际内容与检查点记录逐字节一致，可复用。
	ShardOK ShardVerdict = "ok"
	// ShardOutputMissing：输出文件不存在或太小，装不下该片区间（如断电后输出丢失/截断）。
	ShardOutputMissing ShardVerdict = "output_missing"
	// ShardChecksumMismatch：输出中该片区间存在，但回读摘要与检查点记录不一致（被改写/损坏）。
	ShardChecksumMismatch ShardVerdict = "checksum_mismatch"
)

// ShardHealth 是一个已确认分片的体检结果。
type ShardHealth struct {
	Index    int
	SourceID string
	Start    int64
	End      int64
	Verdict  ShardVerdict
	// Recorded 是检查点记录的 SHA-256；Output 是实际回算出的摘要（缺失时为空）。
	Recorded string
	Output   string
	Reason   string
}

// SourceHealth 是一个来源的体检结果（指纹层面）。
type SourceHealth struct {
	ID       string
	Matches  bool // 当前指纹/声明是否与检查点记录一致
	Recorded Fingerprint
	Current  Fingerprint
	Reason   string
}

// InspectionReport 是只读体检结论；体检不改动输出与检查点。
type InspectionReport struct {
	TaskID  string
	Shards  []ShardHealth // 仅包含检查点中已确认的分片，按索引升序
	Sources []SourceHealth
	// Damaged 是需要重做的分片索引集合（output_missing / checksum_mismatch）。
	Damaged map[int]bool
	// ChangedSources 是指纹或声明与检查点不一致的来源集合。
	ChangedSources map[string]bool
}

// Healthy 在没有坏片、没有来源变化时为 true。
func (r *InspectionReport) Healthy() bool {
	return len(r.Damaged) == 0 && len(r.ChangedSources) == 0
}

// RecoverOptions 控制修复/增量重算的行为。
type RecoverOptions struct {
	// AllowChangedSources 为 true 时，允许“同大小、仅内容改写”的来源变化：
	// 逐片界定影响范围，仅重做受影响分片，未受影响分片继续复用。
	// 来源数量/声明/大小变化等无法可靠界定范围的情况仍会明确失败。
	AllowChangedSources bool
}

// RecoverReport 是一次修复（Recover）的处理结论。
type RecoverReport struct {
	// Reused 未被触碰、直接复用的分片索引（修复前已确认且健康）。
	Reused []int
	// Repaired 原本已确认但被判定有问题、本次重做的分片索引。
	Repaired []int
	// Pending 修复前尚未确认（上次中断）、本次补齐的分片索引。
	Pending []int
	// ChangedSources 被确认发生同大小内容变化并按范围增量重算的来源。
	ChangedSources []string
	FinalHash      string
}

// globalOffsetOf 返回分片 index 在合并输出中的起始偏移（计划由调用方给定）。
func globalOffsetOf(shards []Shard, index int) int64 {
	var off int64
	for i := 0; i < index && i < len(shards); i++ {
		off += shards[i].Length()
	}
	return off
}

// loadCheckpointForMaintenance 加载检查点并做任务硬边界校验。
// 体检与修复共用：检查点缺失/损坏/版本不对/不是本任务都必须明确失败。
func (t *Task) loadCheckpointForMaintenance() (*Checkpoint, error) {
	cp, err := LoadCheckpoint(t.cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, newError(KindNothingToRepair, "",
			"no checkpoint at "+t.cfg.CheckpointPath+"; nothing to inspect or repair (run the task first)",
			ErrNothingToRepair)
	}
	// 任务标识/输出/分片大小属于硬边界，不一致就不是同一份输出的体检/修复。
	if cp.TaskID != t.cfg.TaskID {
		return nil, newError(KindCheckpointMismatch, "",
			fmt.Sprintf("task id mismatch: checkpoint=%q current=%q", cp.TaskID, t.cfg.TaskID),
			ErrCheckpointMismatch)
	}
	if cp.Output != t.cfg.OutputPath {
		return nil, newError(KindCheckpointMismatch, "",
			fmt.Sprintf("output path mismatch: checkpoint=%q current=%q", cp.Output, t.cfg.OutputPath),
			ErrCheckpointMismatch)
	}
	if cp.ShardSize != t.cfg.ShardSize {
		return nil, newError(KindCheckpointMismatch, "",
			fmt.Sprintf("shard size mismatch: checkpoint=%d current=%d", cp.ShardSize, t.cfg.ShardSize),
			ErrCheckpointMismatch)
	}
	return cp, nil
}

// planFromCheckpoint 用检查点冻结的来源状态重建分片计划。
// 输出文件的全局偏移由该计划定义，即使当前来源内容已变化，也必须以它为准。
func planFromCheckpoint(cp *Checkpoint) ([]Shard, error) {
	return PlanShards(cp.Sources, cp.ShardSize)
}

// validateCompletedAgainstPlan 校验完成记录与给定计划的布局一致性。
func validateCompletedAgainstPlan(cp *Checkpoint, shards []Shard) error {
	for idx, cs := range cp.Completed {
		if idx < 0 || idx >= len(shards) {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("completed shard index %d outside checkpoint plan of %d shards", idx, len(shards)),
				ErrCheckpointMismatch)
		}
		sh := shards[idx]
		if sh.SourceID != cs.SourceID || sh.Start != cs.Start || sh.End != cs.End || int64(cs.Bytes) != sh.Length() {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("completed shard %d layout {source=%s [%d,%d) bytes=%d} != plan {source=%s [%d,%d) bytes=%d}",
					idx, cs.SourceID, cs.Start, cs.End, cs.Bytes,
					sh.SourceID, sh.Start, sh.End, sh.Length()), ErrCheckpointMismatch)
		}
	}
	return nil
}

// Inspect 执行只读体检：不创建/截断/写入输出，不写检查点，不推进任何来源游标。
//
// 体检回答两件事：
//  1. 检查点里“已完成”的分片，输出文件对应区间的真实字节是否仍与记录一致
//     （依据：全局偏移回读 + SHA-256 逐片比对）；
//  2. 来源当前指纹是否仍是检查点记录的那份（依据：size + 全量 SHA-256 + mtime）。
//
// 任何坏片与来源变化都体现在返回报告里，并逐条给出判定依据；只有检查点本身
// 无法加载或不属于本任务时才返回错误。
func (t *Task) Inspect(ctx context.Context) (*InspectionReport, error) {
	log := t.log
	cp, err := t.loadCheckpointForMaintenance()
	if err != nil {
		return nil, err
	}
	shards, err := planFromCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	if err := validateCompletedAgainstPlan(cp, shards); err != nil {
		return nil, err
	}

	report := &InspectionReport{
		TaskID:         cp.TaskID,
		Damaged:        map[int]bool{},
		ChangedSources: map[string]bool{},
	}

	// ---- 已确认输出分片：只读回读比对 ----
	outFile, openErr := os.OpenFile(t.cfg.OutputPath, os.O_RDONLY, 0)
	var outSize int64 = -1
	if openErr == nil {
		if info, statErr := outFile.Stat(); statErr == nil {
			outSize = info.Size()
		}
		defer func() { _ = outFile.Close() }()
	} else if !os.IsNotExist(openErr) {
		// 权限等问题要明确报错，而不是把所有片误报为丢失。
		if os.IsPermission(openErr) {
			return nil, newError(KindPermission, "", "output open not permitted: "+t.cfg.OutputPath, openErr)
		}
		return nil, newError(KindSourceUnreadable, "", "output open failed: "+t.cfg.OutputPath, openErr)
	}

	indexes := make([]int, 0, len(cp.Completed))
	for idx := range cp.Completed {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	for _, idx := range indexes {
		cs := cp.Completed[idx]
		h := ShardHealth{
			Index: idx, SourceID: cs.SourceID, Start: cs.Start, End: cs.End,
			Verdict: ShardOK, Recorded: cs.Checksum,
		}
		off := globalOffsetOf(shards, idx)
		switch {
		case outFile == nil:
			h.Verdict = ShardOutputMissing
			h.Reason = fmt.Sprintf("output file %s does not exist; recorded shard occupies output [%d,%d)",
				t.cfg.OutputPath, off, off+int64(cs.Bytes))
		case outSize < 0:
			h.Verdict = ShardOutputMissing
			h.Reason = "output file cannot be stat'd"
		case off+int64(cs.Bytes) > outSize:
			h.Verdict = ShardOutputMissing
			h.Reason = fmt.Sprintf("output too small: need [%d,%d), file size=%d (output truncated/lost after crash?)",
				off, off+int64(cs.Bytes), outSize)
		default:
			buf := make([]byte, cs.Bytes)
			if _, rerr := readFullAt(ctx, outFile, buf, off); rerr != nil {
				return nil, newError(KindSourceUnreadable, cs.SourceID,
					fmt.Sprintf("re-read completed shard %d failed during inspection", idx), rerr)
			}
			h.Output = checksumBytes(buf)
			if h.Output != cs.Checksum {
				h.Verdict = ShardChecksumMismatch
				h.Reason = fmt.Sprintf("output [%d,%d) sha256=%s != checkpoint recorded sha256=%s (bytes altered/corrupted)",
					off, off+int64(cs.Bytes), h.Output, cs.Checksum)
			}
		}
		if h.Verdict != ShardOK {
			report.Damaged[idx] = true
		}
		report.Shards = append(report.Shards, h)
		log.InfoContext(ctx, "healthcheck shard",
			slog.Int("index", h.Index), slog.String("source", h.SourceID),
			slog.Int64("start", h.Start), slog.Int64("end", h.End),
			slog.String("verdict", string(h.Verdict)), slog.String("basis", h.Reason))
	}

	// ---- 来源指纹复核（只读；单个来源不可读只影响该来源的结论） ----
	specByID := make(map[string]SourceSpec, len(t.cfg.SourceSpecs))
	for _, sp := range t.cfg.SourceSpecs {
		specByID[sp.ID] = sp
	}
	for _, want := range cp.Sources {
		sh := SourceHealth{ID: want.ID, Matches: true, Recorded: want.Fingerprint}
		sp, declared := specByID[want.ID]
		switch {
		case !declared:
			sh.Matches = false
			sh.Reason = "source no longer declared in current task config"
		case sp.Kind != want.Kind:
			sh.Matches = false
			sh.Reason = fmt.Sprintf("source kind changed: checkpoint=%s current=%s", want.Kind, sp.Kind)
		default:
			fp, ferr := computeFingerprint(ctx, sp.Path, sp.ID)
			if ferr != nil {
				sh.Matches = false
				sh.Reason = "current fingerprint unavailable: " + ferr.Error()
			} else {
				sh.Current = fp
				if fp != want.Fingerprint {
					sh.Matches = false
					sh.Reason = fmt.Sprintf("fingerprint changed: checkpoint={size=%d hash=%s mod=%s} current={size=%d hash=%s mod=%s}",
						want.Fingerprint.Size, want.Fingerprint.Hash, want.Fingerprint.Mod,
						fp.Size, fp.Hash, fp.Mod)
				}
			}
		}
		if !sh.Matches {
			report.ChangedSources[want.ID] = true
		}
		report.Sources = append(report.Sources, sh)
		log.InfoContext(ctx, "healthcheck source",
			slog.String("source", sh.ID), slog.Bool("matches", sh.Matches),
			slog.String("basis", sh.Reason))
	}

	log.InfoContext(ctx, "healthcheck complete",
		slog.Int("completed_shards", len(cp.Completed)),
		slog.Int("damaged_shards", len(report.Damaged)),
		slog.Int("changed_sources", len(report.ChangedSources)),
		slog.Bool("healthy", report.Healthy()),
		slog.String("basis", "read-only: no output or checkpoint was modified"))
	return report, nil
}

// readRangeReadonly 以一个独立的只读文件描述符读取 [start,end)。
// 用于修复前的影响范围界定：它不触碰任务实际打开的顺序源（不推进其游标），
// 从而保证后续流水线仍能 ResumeAt 到复用前缀。
func readRangeReadonly(ctx context.Context, path, sourceID string, start, end int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsPermission(err) {
			return nil, newError(KindPermission, sourceID, "open read-only not permitted: "+path, err)
		}
		return nil, newError(KindSourceUnreadable, sourceID, "open failed: "+path, err)
	}
	defer f.Close()
	n := end - start
	buf := make([]byte, n)
	if n == 0 {
		return buf, nil
	}
	if _, err := readFullAt(ctx, f, buf, start); err != nil {
		return nil, newError(KindSourceUnreadable, sourceID,
			fmt.Sprintf("read range [%d,%d) failed", start, end), err)
	}
	return buf, nil
}

// verifyOutputShards 只读回读输出中给定已确认分片，返回判定结果（不返回错误即全部 OK）。
// 输出文件不存在/太小 => ShardOutputMissing；摘要不符 => ShardChecksumMismatch。
// 与 Inspect 共享同一判定依据，确保“体检报告什么，修复就处理什么”。
func (t *Task) verifyOutputShards(ctx context.Context, cp *Checkpoint, shards []Shard, indexes []int) ([]ShardHealth, error) {
	out := make([]ShardHealth, 0, len(indexes))
	outFile, openErr := os.OpenFile(t.cfg.OutputPath, os.O_RDONLY, 0)
	var outSize int64 = -1
	if openErr == nil {
		if info, statErr := outFile.Stat(); statErr == nil {
			outSize = info.Size()
		}
		defer func() { _ = outFile.Close() }()
	} else if !os.IsNotExist(openErr) {
		if os.IsPermission(openErr) {
			return nil, newError(KindPermission, "", "output open not permitted: "+t.cfg.OutputPath, openErr)
		}
		return nil, newError(KindSourceUnreadable, "", "output open failed: "+t.cfg.OutputPath, openErr)
	}
	for _, idx := range indexes {
		cs := cp.Completed[idx]
		h := ShardHealth{Index: idx, SourceID: cs.SourceID, Start: cs.Start, End: cs.End,
			Verdict: ShardOK, Recorded: cs.Checksum}
		off := globalOffsetOf(shards, idx)
		switch {
		case outFile == nil:
			h.Verdict = ShardOutputMissing
			h.Reason = "output file does not exist"
		case outSize < 0:
			h.Verdict = ShardOutputMissing
			h.Reason = "output file cannot be stat'd"
		case off+int64(cs.Bytes) > outSize:
			h.Verdict = ShardOutputMissing
			h.Reason = fmt.Sprintf("output too small: need [%d,%d), file size=%d", off, off+int64(cs.Bytes), outSize)
		default:
			buf := make([]byte, cs.Bytes)
			if _, rerr := readFullAt(ctx, outFile, buf, off); rerr != nil {
				return nil, newError(KindSourceUnreadable, cs.SourceID,
					fmt.Sprintf("re-read completed shard %d failed", idx), rerr)
			}
			h.Output = checksumBytes(buf)
			if h.Output != cs.Checksum {
				h.Verdict = ShardChecksumMismatch
				h.Reason = fmt.Sprintf("output sha256=%s != recorded sha256=%s at output [%d,%d)",
					h.Output, cs.Checksum, off, off+int64(cs.Bytes))
			}
		}
		out = append(out, h)
	}
	return out, nil
}

// sortIndexes 返回 map 键的升序切片。
func sortIndexes(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// invalidate 从检查点中移除给定分片的完成记录（标记为需要重做）。
// 移除而不是覆盖：流水线重做完会以新校验和重新确认，绝不保留矛盾记录。
func invalidate(cp *Checkpoint, indexes map[int]bool) {
	for idx := range indexes {
		delete(cp.Completed, idx)
	}
}

// Recover 执行修复：只重做体检认定有问题的部分，其余分片原样复用。
//
// 判定与处理：
//
//   - 输出坏片（丢失/截断/摘要不符）：从检查点移除这些分片的确认，重新读取、
//     在相同全局偏移重写并重新确认；其余健康分片直接复用。
//   - 来源变化（默认）：明确 KindCheckpointMismatch，拒绝把新旧内容拼成一份。
//   - 来源变化且 AllowChangedSources：仅当所有来源的数量/声明/大小与检查点完全一致
//     （允许同大小内容改写）时，逐片从当前来源只读回算并与记录比对，仅受影响分片
//     重做；顺序源从其第一个受影响分片起重做（游标单调约束）。一旦来源数量/声明/
//     大小有变（分片布局不再可靠对应），明确 KindSourceLayoutChanged。
//
// 幂等：再次 Recover 时所有片均健康且无来源差异 => reused=全部、repared=空，
// 输出与检查点不被改动（除刷新 final_hash 外结果不变）。
func (t *Task) Recover(ctx context.Context, opts RecoverOptions) (*RecoverReport, error) {
	log := t.log
	cp, err := t.loadCheckpointForMaintenance()
	if err != nil {
		return nil, err
	}
	shards, err := planFromCheckpoint(cp)
	if err != nil {
		return nil, err
	}
	if err := validateCompletedAgainstPlan(cp, shards); err != nil {
		return nil, err
	}

	// ---- 1. 声明级匹配：来源数量/顺序/id/kind 必须一致（硬边界） ----
	if len(t.cfg.SourceSpecs) != len(cp.Sources) {
		return nil, newError(KindSourceLayoutChanged, "",
			fmt.Sprintf("source count changed: checkpoint=%d current=%d; cannot map shards reliably, refusing to mix old and new data",
				len(cp.Sources), len(t.cfg.SourceSpecs)), ErrSourceLayoutChanged)
	}
	specByID := make(map[string]SourceSpec, len(t.cfg.SourceSpecs))
	for i, sp := range t.cfg.SourceSpecs {
		want := cp.Sources[i]
		if sp.ID != want.ID {
			return nil, newError(KindSourceLayoutChanged, want.ID,
				fmt.Sprintf("source #%d id changed: checkpoint=%q current=%q", i, want.ID, sp.ID),
				ErrSourceLayoutChanged)
		}
		if sp.Kind != want.Kind {
			return nil, newError(KindSourceLayoutChanged, sp.ID,
				fmt.Sprintf("source %q kind changed: checkpoint=%s current=%s", sp.ID, want.Kind, sp.Kind),
				ErrSourceLayoutChanged)
		}
		specByID[sp.ID] = sp
	}

	// ---- 2. 指纹复核：哪些来源变了 ----
	currentFP := make(map[string]Fingerprint, len(cp.Sources))
	var changedList []string
	for _, want := range cp.Sources {
		sp := specByID[want.ID]
		fp, ferr := computeFingerprint(ctx, sp.Path, sp.ID)
		if ferr != nil {
			// 当前指纹都无法取得，无法可靠界定影响范围 => 明确失败。
			return nil, ferr
		}
		currentFP[want.ID] = fp
		if fp != want.Fingerprint {
			changedList = append(changedList, want.ID)
			log.InfoContext(ctx, "repair: source changed since checkpoint",
				slog.String("source", want.ID),
				slog.Int64("checkpoint_size", want.Fingerprint.Size),
				slog.Int64("current_size", fp.Size),
				slog.String("basis",
					fmt.Sprintf("fingerprint changed: checkpoint hash=%s current hash=%s",
						want.Fingerprint.Hash, fp.Hash)))
		}
	}

	// ---- 3. 影响范围界定 ----
	needRedo := map[int]bool{}

	// 3a. 输出损坏（依据检查点计划回读输出比对）
	completedIdx := make([]int, 0, len(cp.Completed))
	for idx := range cp.Completed {
		completedIdx = append(completedIdx, idx)
	}
	sort.Ints(completedIdx)
	health, err := t.verifyOutputShards(ctx, cp, shards, completedIdx)
	if err != nil {
		return nil, err
	}
	for _, h := range health {
		if h.Verdict != ShardOK {
			needRedo[h.Index] = true
			log.InfoContext(ctx, "repair: shard will be redone",
				slog.Int("index", h.Index), slog.String("source", h.SourceID),
				slog.String("reason", string(h.Verdict)), slog.String("basis", h.Reason))
		}
	}

	// 3b. 来源变化
	sort.Strings(changedList)
	if len(changedList) > 0 && !opts.AllowChangedSources {
		return nil, newError(KindCheckpointMismatch, "",
			fmt.Sprintf("sources changed since checkpoint %v; rerun Recover with AllowChangedSources for same-size content edits, or restart from scratch",
				changedList), ErrCheckpointMismatch)
	}

	// 按来源聚合分片，便于顺序源“首个受影响分片起全部重做”。
	shardsBySource := map[string][]Shard{}
	for _, sh := range shards {
		shardsBySource[sh.SourceID] = append(shardsBySource[sh.SourceID], sh)
	}

	for _, sid := range changedList {
		want := cp.Sources[0]
		for _, s := range cp.Sources {
			if s.ID == sid {
				want = s
			}
		}
		// 大小变化 => 分片数量/边界无法与既有输出偏移可靠对应，拒绝拼接。
		if currentFP[sid].Size != want.Fingerprint.Size {
			return nil, newError(KindSourceLayoutChanged, sid,
				fmt.Sprintf("source %q size changed: checkpoint=%d current=%d; shard layout cannot be mapped reliably, refusing incremental reuse",
					sid, want.Fingerprint.Size, currentFP[sid].Size), ErrSourceLayoutChanged)
		}
		// 大小一致 => 分片边界与全局偏移完全不变；把检查点中的来源指纹更新为当前值，
		// 使修复完成后的检查点反映“输出当前对应的那份来源”，后续 Run/Inspect 不再误报变化。
		want.Fingerprint = currentFP[sid]
		for i := range cp.Sources {
			if cp.Sources[i].ID == sid {
				cp.Sources[i].Fingerprint = currentFP[sid]
			}
		}
		sp := specByID[sid]
		// 同大小内容改写：逐片从“当前”来源只读回算，与检查点记录比对。
		var firstAffected int = -1
		for _, sh := range shardsBySource[sid] {
			if !cp.IsCompleted(sh.Index) {
				continue // 未完成的片本来就要重做
			}
			data, rerr := readRangeReadonly(ctx, sp.Path, sid, sh.Start, sh.End)
			if rerr != nil {
				return nil, rerr
			}
			cur := checksumBytes(data)
			if cur != cp.Completed[sh.Index].Checksum {
				if firstAffected < 0 {
					firstAffected = sh.Index
				}
				log.InfoContext(ctx, "repair: affected by source change",
					slog.Int("index", sh.Index), slog.String("source", sid),
					slog.Int64("start", sh.Start), slog.Int64("end", sh.End),
					slog.String("recorded", cp.Completed[sh.Index].Checksum),
					slog.String("current", cur),
					slog.String("basis", "recomputed from current source content"))
				switch want.Kind {
				case Sequential:
					// 顺序源：标记从该片起的全部已确认片（游标不能回退/跳过）。
				default:
					needRedo[sh.Index] = true
				}
			}
		}
		if want.Kind == Sequential && firstAffected >= 0 {
			for _, sh := range shardsBySource[sid] {
				if sh.Index >= firstAffected && cp.IsCompleted(sh.Index) {
					needRedo[sh.Index] = true
				}
			}
			log.InfoContext(ctx, "repair: sequential source redo suffix",
				slog.String("source", sid), slog.Int("from_index", firstAffected),
				slog.String("basis", "sequential cursor cannot rewind; redo confirmed suffix"))
		}
	}

	// ---- 4. 失效并落盘（在触碰任何输出之前先持久化“需要重做”的决定） ----
	redoList := sortIndexes(needRedo)
	reusedList := make([]int, 0, len(cp.Completed))
	pendingList := make([]int, 0, len(shards)-len(cp.Completed))
	for idx := range cp.Completed {
		if !needRedo[idx] {
			reusedList = append(reusedList, idx)
		}
	}
	for _, sh := range shards {
		if !cp.IsCompleted(sh.Index) {
			pendingList = append(pendingList, sh.Index)
		}
	}
	sort.Ints(reusedList)
	sort.Ints(pendingList)

	totalShards := len(shards)
	invalidate(cp, needRedo)
	cp.FinalHash = ""
	if err := cp.Save(t.cfg.CheckpointPath); err != nil {
		return nil, err
	}
	log.InfoContext(ctx, "repair plan",
		slog.Int("reused", len(reusedList)), slog.Int("redone", len(redoList)),
		slog.Int("pending", len(pendingList)),
		slog.Int("shards_total", totalShards), slog.Any("redo_indexes", redoList),
		slog.Any("changed_sources", changedList),
		slog.String("basis", "damaged output + source-change impact analysis"))

	// ---- 5. 复用既有 Run 的打开来源/恢复游标/流水线/最终校验 ----
	t.changedSources = map[string]bool{}
	for _, sid := range changedList {
		t.changedSources[sid] = true
	}
	if err := t.executeRecoverRun(ctx, cp); err != nil {
		return nil, err
	}

	return &RecoverReport{
		Reused:         reusedList,
		Repaired:       redoList,
		Pending:        pendingList,
		ChangedSources: changedList,
		FinalHash:      cp.FinalHash,
	}, nil
}
