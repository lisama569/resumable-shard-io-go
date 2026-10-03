// Package shardio 的 doctor.go 提供崩溃后的“只读体检 + 定向修复”能力：
//
//   - Doctor：不改动输出文件与检查点的只读体检。对每个已确认分片同时比对三方
//     依据——检查点记录的校验和、输出文件对应区间回读的校验和、当前来源同区间
//     重读的校验和——并给出确定的分片级结论与原因；
//   - Repair：先体检，再只重做被判定有问题的分片。影响范围能逐片证实时只动坏片，
//     无法可靠证实时整体拒绝（绝不拼接新旧内容）。体检与修复都可重复执行且幂等。
package shardio

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
)

// ShardStatus 是体检对单个已确认分片的判定结论。
type ShardStatus string

const (
	// StatusOK：输出区间与来源内容均与检查点记录一致，可直接复用。
	StatusOK ShardStatus = "ok"
	// StatusOutputDamaged：检查点完好且来源未变，但输出区间对不上（如 kill -9 后
	// 外部损坏/截断），只需重读本分片即可修复。
	StatusOutputDamaged ShardStatus = "output_damaged"
	// StatusSourceChanged：来源内容在同大小前提下发生变化，且本分片内容确实受影响，
	// 需用当前来源重做该分片；其判定依据是“当前来源该区间校验和 != 记录校验和”。
	StatusSourceChanged ShardStatus = "source_changed"
	// StatusIndeterminable：无法可靠判断本分片是否可复用（来源大小/集合/顺序变化等
	// 会改变分片布局），修复必须整体拒绝，不能拼接新旧内容。
	StatusIndeterminable ShardStatus = "indeterminable"
)

// ShardDiagnosis 是单个已确认分片的体检明细（含判定依据，供日志与报告使用）。
type ShardDiagnosis struct {
	Index       int
	SourceID    string
	Start       int64
	End         int64
	Status      ShardStatus
	Recorded    string // 检查点记录的分片校验和
	OutputHash  string // 从输出文件回读的校验和（输出区间不可读时为空）
	CurrentHash string // 从当前来源重读的校验和（来源指纹未变时为空）
	Reason      string // 人类可读的判定依据
}

// DoctorReport 是只读体检报告：不改动任何输出与检查点。
type DoctorReport struct {
	TaskID         string
	TotalCompleted int
	ByStatus       map[ShardStatus]int
	Shards         []ShardDiagnosis // 按分片序号升序
	// Repairable 为 true 表示所有问题都落在“可可靠界定影响范围”的分片内，
	// Repair 只会重做这些分片；为 false 时 Repair 必须明确拒绝。
	Repairable bool
	// ChangedSources 记录指纹发生变化的来源 ID（内容级变化与大小级变化都计入）。
	ChangedSources []string
}

// RepairReport 是修复执行报告：只重做被判定有问题的分片，并说明复用/重做的依据。
type RepairReport struct {
	Repaired  []int // 被重做的分片序号（升序）
	Reused    []int // 被复用的分片序号（升序）
	Rejected  bool
	FinalHash string
	Doctor    *DoctorReport
}

// Doctor 在不触碰输出文件与检查点的前提下，对已确认分片做只读体检。
//
// 判定依据（逐片，按分片序号升序执行）：
//  1. 记录值 R：检查点中该片数据的 SHA-256；
//  2. 输出值 O：从输出文件“旧分片计划”下的全局偏移回读该片的 SHA-256；
//  3. 当前值 C：从当前来源按 [start,end) 重读该片的 SHA-256
//     （顺序型来源走不推进游标的 pread 审计通道）。
//
// 结论：
//   - ok：O==R 且 C==R，直接复用；
//   - output_damaged：C==R（来源未变）但 O!=R / 输出区间缺失，只需重做本片；
//   - source_changed：来源“同大小”但 C!=R，本片内容确实受影响，需以当前来源重做；
//   - indeterminable：来源大小变化、或前序来源大小变化改变本片输出偏移等，
//     无法逐片证实安全边界，修复必须整体拒绝。
//
// 体检本身只读：输出以 O_RDONLY 打开，检查点绝不写回。结构性不兼容
// （任务/输出/分片大小/来源声明变化、检查点损坏）返回错误而非报告。
func (t *Task) Doctor(ctx context.Context) (*DoctorReport, error) {
	if err := t.ensureOpen(ctx); err != nil {
		return nil, err
	}
	defer t.closeSources() // 体检为独立只读动作，返回时释放真实文件句柄
	cp, err := LoadCheckpoint(t.cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, newError(KindInvalidArgument, "",
			"doctor: no checkpoint at "+t.cfg.CheckpointPath+" (nothing completed to diagnose)", nil)
	}
	if err := t.validateStructure(cp); err != nil {
		return nil, err
	}
	return t.diagnose(ctx, cp)
}

// validateStructure 只校验不涉及“来源内容是否变化”的结构字段；
// 指纹差异属于体检内容，不是结构性失败。
func (t *Task) validateStructure(cp *Checkpoint) error {
	if cp.TaskID != t.cfg.TaskID {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("task id mismatch: checkpoint=%q current=%q", cp.TaskID, t.cfg.TaskID),
			ErrCheckpointMismatch)
	}
	if cp.Output != t.cfg.OutputPath {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("output path mismatch: checkpoint=%q current=%q", cp.Output, t.cfg.OutputPath),
			ErrCheckpointMismatch)
	}
	if cp.ShardSize != t.cfg.ShardSize {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("shard size mismatch: checkpoint=%d current=%d", cp.ShardSize, t.cfg.ShardSize),
			ErrCheckpointMismatch)
	}
	if len(cp.Sources) != len(t.states) {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("source count mismatch: checkpoint=%d current=%d", len(cp.Sources), len(t.states)),
			ErrCheckpointMismatch)
	}
	for i, want := range cp.Sources {
		got := t.states[i]
		if want.ID != got.ID || want.Kind != got.Kind || want.MaxConcurrency != got.MaxConcurrency {
			return newError(KindCheckpointMismatch, got.ID,
				fmt.Sprintf("source #%d declaration mismatch: checkpoint={%s %s max=%d} current={%s %s max=%d}",
					i, want.ID, want.Kind, want.MaxConcurrency, got.ID, got.Kind, got.MaxConcurrency),
				ErrCheckpointMismatch)
		}
	}
	return nil
}

// ensureOpen 惰性打开来源并规划当前分片；重复调用复用已打开的句柄。
// closeSources 会清空 t.sources，此时 states/shards 也必须一并重建，
// 否则再次打开会向残留切片追加，导致来源数量重复。
func (t *Task) ensureOpen(ctx context.Context) error {
	if t.sources != nil {
		return nil
	}
	t.states = nil
	t.shards = nil
	return t.openSources(ctx)
}

// diagnose 执行逐片体检并汇总报告（要求已通过结构校验、来源已打开）。
func (t *Task) diagnose(ctx context.Context, cp *Checkpoint) (*DoctorReport, error) {
	log := t.log
	// 旧分片计划：来源大小变化后，输出中已确认字节的位置只能按“旧布局”定位。
	oldShards, err := PlanShards(cp.Sources, cp.ShardSize)
	if err != nil {
		return nil, err
	}
	oldOffset := make(map[int]int64, len(oldShards))
	var off int64
	for _, sh := range oldShards {
		oldOffset[sh.Index] = off
		off += sh.Length()
	}
	newByIndex := make(map[int]Shard, len(t.shards))
	for _, sh := range t.shards {
		newByIndex[sh.Index] = sh
	}
	oldState := make(map[string]SourceState, len(cp.Sources))
	order := make(map[string]int, len(t.states))
	sizeChanged := map[string]bool{}
	contentChanged := map[string]bool{}
	for _, st := range cp.Sources {
		oldState[st.ID] = st
	}
	for i, cur := range t.states {
		order[cur.ID] = i
		old := oldState[cur.ID]
		switch {
		case cur.Fingerprint.Size != old.Fingerprint.Size:
			sizeChanged[cur.ID] = true
		case cur.Fingerprint.Hash != old.Fingerprint.Hash || cur.Fingerprint.Mod != old.Fingerprint.Mod:
			contentChanged[cur.ID] = true
		}
	}

	indexes := make([]int, 0, len(cp.Completed))
	for idx := range cp.Completed {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)

	// 输出文件只读打开；文件不存在不致命（所有已确认片都会判为输出损坏）。
	out, oerr := os.OpenFile(t.cfg.OutputPath, os.O_RDONLY, 0)
	var outInfo os.FileInfo
	if oerr == nil {
		defer func() { _ = out.Close() }()
		outInfo, _ = out.Stat()
	} else if !os.IsNotExist(oerr) {
		if os.IsPermission(oerr) {
			return nil, newError(KindPermission, "", "doctor: output not readable: "+t.cfg.OutputPath, oerr)
		}
		return nil, newError(KindSourceUnreadable, "", "doctor: output open failed: "+t.cfg.OutputPath, oerr)
	}

	srcByID := map[string]Source{}
	for _, s := range t.sources {
		srcByID[s.ID()] = s
	}

	rep := &DoctorReport{
		TaskID:         cp.TaskID,
		TotalCompleted: len(cp.Completed),
		ByStatus:       map[ShardStatus]int{},
	}

	for _, idx := range indexes {
		cs := cp.Completed[idx]
		d := ShardDiagnosis{
			Index: idx, SourceID: cs.SourceID, Start: cs.Start, End: cs.End,
			Recorded: cs.Checksum,
		}

		// ---- 输出侧（按旧布局偏移回读）----
		outOff := oldOffset[idx]
		switch {
		case oerr != nil: // 文件不存在
			d.Status = StatusOutputDamaged
			d.Reason = "output file does not exist"
		case outInfo.Size() < outOff+int64(cs.Bytes):
			d.Status = StatusOutputDamaged
			d.Reason = fmt.Sprintf("output too small: need [%d,%d), file size=%d",
				outOff, outOff+int64(cs.Bytes), outInfo.Size())
		default:
			buf := make([]byte, cs.Bytes)
			if _, rerr := readFullAt(ctx, out, buf, outOff); rerr != nil {
				return nil, newError(KindSourceUnreadable, cs.SourceID,
					fmt.Sprintf("doctor: re-read output shard %d failed", idx), rerr)
			}
			d.OutputHash = checksumBytes(buf)
		}

		// ---- 来源侧与影响范围判定 ----
		// 1) 来源自身大小变化：分片数量/边界变化，无法逐片安全复用。
		if sizeChanged[cs.SourceID] {
			d.Status = StatusIndeterminable
			d.Reason = fmt.Sprintf("source %s size changed %d->%d: shard layout changes, safe reuse cannot be proven per shard",
				cs.SourceID, oldState[cs.SourceID].Fingerprint.Size, t.currentSize(cs.SourceID))
		} else {
			// 2) 前序来源大小变化会整体平移本片的输出偏移，旧输出位置失效。
			for sid := range sizeChanged {
				if order[sid] < order[cs.SourceID] {
					d.Status = StatusIndeterminable
					d.Reason = fmt.Sprintf("preceding source %s size changed: global output offset of shard %d shifts", sid, idx)
					break
				}
			}
		}

		if d.Status == "" {
			// 同大小前提下，新旧计划中本片布局必须一致（结构性防御）。
			ns, ok := newByIndex[idx]
			if !ok || ns.SourceID != cs.SourceID || ns.Start != cs.Start || ns.End != cs.End || ns.Length() != int64(cs.Bytes) {
				d.Status = StatusIndeterminable
				d.Reason = fmt.Sprintf("completed shard %d layout not present identically in current plan", idx)
			} else {
				cur, serr := t.auditReadShard(ctx, srcByID[cs.SourceID], cs.SourceID, cs.Start, cs.End)
				if serr != nil {
					return nil, serr // 来源不可读等：体检无法得出结论，明确失败
				}
				d.CurrentHash = cur
				switch {
				case cur != cs.Checksum:
					d.Status = StatusSourceChanged
					d.Reason = fmt.Sprintf("current source hash %s != recorded %s (same size %d): shard content changed",
						cur, cs.Checksum, oldState[cs.SourceID].Fingerprint.Size)
				case d.OutputHash != cs.Checksum:
					d.Status = StatusOutputDamaged
					if d.OutputHash != "" {
						d.Reason = fmt.Sprintf("source hash %s matches recorded, output hash %s != recorded %s",
							cur, d.OutputHash, cs.Checksum)
					}
					// OutputHash == "" 时原因已在输出侧给出（如文件不存在/太小）。
				default:
					d.Status = StatusOK
					d.Reason = "output and current source both match recorded checksum"
				}
			}
		}

		rep.ByStatus[d.Status]++
		rep.Shards = append(rep.Shards, d)
		log.InfoContext(ctx, "doctor shard diagnosis",
			slog.Int("index", d.Index), slog.String("source", d.SourceID),
			slog.Int64("start", d.Start), slog.Int64("end", d.End),
			slog.String("status", string(d.Status)),
			slog.String("recorded", d.Recorded), slog.String("output_hash", d.OutputHash),
			slog.String("current_hash", d.CurrentHash), slog.String("basis", d.Reason))
	}

	for sid := range sizeChanged {
		rep.ChangedSources = append(rep.ChangedSources, sid)
	}
	for sid := range contentChanged {
		rep.ChangedSources = append(rep.ChangedSources, sid)
	}
	sort.Strings(rep.ChangedSources)
	rep.Repairable = rep.ByStatus[StatusIndeterminable] == 0

	log.InfoContext(ctx, "doctor report",
		slog.Int("completed", rep.TotalCompleted),
		slog.Int("ok", rep.ByStatus[StatusOK]),
		slog.Int("output_damaged", rep.ByStatus[StatusOutputDamaged]),
		slog.Int("source_changed", rep.ByStatus[StatusSourceChanged]),
		slog.Int("indeterminable", rep.ByStatus[StatusIndeterminable]),
		slog.Bool("repairable", rep.Repairable),
		slog.Any("changed_sources", rep.ChangedSources),
		slog.String("basis", "read-only: neither output nor checkpoint was modified"))
	return rep, nil
}

func (t *Task) currentSize(sourceID string) int64 {
	for _, st := range t.states {
		if st.ID == sourceID {
			return st.Fingerprint.Size
		}
	}
	return -1
}

// auditReadShard 重读一个已确认分片的当前内容；顺序型来源使用不推进游标的
// 审计读取，随机型来源直接 ReadShard（读取前后自带身份复核）。
func (t *Task) auditReadShard(ctx context.Context, src Source, sourceID string, start, end int64) (string, error) {
	switch s := src.(type) {
	case *SequentialFileSource:
		data, err := s.ReadShardAudit(ctx, start, end)
		if err != nil {
			return "", err
		}
		return checksumBytes(data), nil
	default:
		data, err := src.ReadShard(ctx, start, end)
		if err != nil {
			return "", err
		}
		return checksumBytes(data), nil
	}
}

// Repair 先做只读体检，再只重做被判定有问题的分片。
//
// 语义约定：
//   - 体检发现 indeterminable（影响范围无法逐片证实）时：不改动任何文件，
//     返回带 Rejected 报告的 KindRepairNotPossible，调用方应整体重跑；
//   - output_damaged：从当前来源重读该片、覆盖输出对应区间；
//   - source_changed（同大小内容变化）：以当前来源内容重做该片；
//   - 顺序型来源一旦有任何前序分片需要重做，其后继分片一并重做
//     （游标不能回退/跳跃，这是顺序设备可可靠界定的最小重做范围）；
//   - 其余 ok 分片一律复用，不重读、不覆写；
//   - 成功后最终输出通过与普通 Run 完全相同的串行等价校验（全量大小 +
//     全量 SHA-256 + 逐片回读 + 收尾指纹），结果与整体重跑逐字节一致；
//   - 无问题或重复执行：幂等空操作，不重复处理、不改坏好片。
func (t *Task) Repair(ctx context.Context) (rr *RepairReport, retErr error) {
	log := t.log
	if err := t.ensureOpen(ctx); err != nil {
		return nil, err
	}
	defer t.closeSources() // 修复结束后释放真实来源句柄
	// Repair 内部打开的输出句柄在返回时关闭并清空，使同一 Task 实例可再次调用。
	defer func() {
		if t.out != nil {
			_ = t.out.Close()
			t.out = nil
		}
	}()
	cp, err := LoadCheckpoint(t.cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, newError(KindInvalidArgument, "",
			"repair: no checkpoint at "+t.cfg.CheckpointPath+"; use Run for the first execution", nil)
	}
	if err := t.validateStructure(cp); err != nil {
		return nil, err
	}
	report, err := t.diagnose(ctx, cp)
	if err != nil {
		return nil, err
	}
	out := &RepairReport{Doctor: report}
	if !report.Repairable {
		out.Rejected = true
		for _, d := range report.Shards {
			if d.Status == StatusIndeterminable {
				log.ErrorContext(ctx, "repair rejected",
					slog.Int("index", d.Index), slog.String("source", d.SourceID),
					slog.String("basis", d.Reason))
			}
		}
		return out, newError(KindRepairNotPossible, "",
			fmt.Sprintf("repair scope cannot be determined safely: %d shard(s) indeterminable; "+
				"a full rerun is required (see doctor report)",
				report.ByStatus[StatusIndeterminable]), ErrRepairNotPossible)
	}

	// ---- 计算需要重做的分片集合 ----
	redo := map[int]bool{}
	for _, d := range report.Shards {
		if d.Status == StatusOutputDamaged || d.Status == StatusSourceChanged {
			redo[d.Index] = true
		}
	}
	// 顺序源扩展：任一前序片重做 => 该来源该片之后的所有已确认片重做，
	// 以保证恢复时游标看到的仍是连续已确认前缀。
	bySource := map[string][]Shard{}
	for _, sh := range t.shards {
		bySource[sh.SourceID] = append(bySource[sh.SourceID], sh)
	}
	for _, src := range t.sources {
		if src.Kind() != Sequential {
			continue
		}
		hit := false
		for _, sh := range bySource[src.ID()] {
			if hit && cp.IsCompleted(sh.Index) && !redo[sh.Index] {
				redo[sh.Index] = true
				log.InfoContext(ctx, "repair redo-set expanded",
					slog.Int("index", sh.Index), slog.String("source", src.ID()),
					slog.String("basis", "sequential source: later shard must be redone after an earlier redo (cursor cannot jump)"))
			}
			if redo[sh.Index] {
				hit = true
			}
		}
	}

	// 幂等快速路径：没有任何需要重做的分片。仍走最终校验以证明成品等价。
	if len(redo) == 0 {
		log.InfoContext(ctx, "repair no-op",
			slog.Int("reused", report.ByStatus[StatusOK]),
			slog.String("basis", "all completed shards diagnosed ok; nothing rewritten"))
		return t.finishRepair(ctx, cp, out, nil)
	}

	// ---- 撤销坏片的完成记录（随后用与普通恢复完全相同的路径续跑）----
	redone := make([]int, 0, len(redo))
	for idx := range redo {
		redone = append(redone, idx)
	}
	sort.Ints(redone)
	for _, idx := range redone {
		cs := cp.Completed[idx]
		if cp.ForgetCompleted(idx) {
			log.InfoContext(ctx, "repair invalidating shard",
				slog.Int("index", idx), slog.String("source", cs.SourceID),
				slog.Int64("start", cs.Start), slog.Int64("end", cs.End),
				slog.String("status", string(report.statusOf(idx))),
				slog.String("basis", "diagnosis: recorded checksum not reproducible from output/current source"))
		}
	}

	// 打开输出（读写；与 Run 相同的定点覆盖语义）。
	f, ferr := os.OpenFile(t.cfg.OutputPath, os.O_RDWR|os.O_CREATE, 0o600)
	if ferr != nil {
		if os.IsPermission(ferr) {
			return nil, newError(KindPermission, "", "repair: output open not permitted: "+t.cfg.OutputPath, ferr)
		}
		return nil, newError(KindSourceUnreadable, "", "repair: output open failed: "+t.cfg.OutputPath, ferr)
	}
	t.out = f

	if err := t.validateCompletedLayout(cp); err != nil {
		return nil, err
	}
	// 被复用片的输出已经过体检三方核对；这里再对复用片做一次回读复核，
	// 关闭“体检完成到真正续跑之间”的变化窗口；若期间又变了则本次修复失败。
	for _, d := range report.Shards {
		if redo[d.Index] {
			continue
		}
		cs := cp.Completed[d.Index]
		off := t.globalOffset(cs.Index)
		info, _ := f.Stat()
		if info == nil || info.Size() < off+int64(cs.Bytes) {
			return nil, newError(KindChecksumMismatch, cs.SourceID,
				fmt.Sprintf("repair: reused shard %d output vanished between diagnosis and repair", d.Index),
				ErrChecksumMismatch)
		}
		buf := make([]byte, cs.Bytes)
		if _, rerr := readFullAt(ctx, f, buf, off); rerr != nil {
			return nil, newError(KindSourceUnreadable, cs.SourceID,
				fmt.Sprintf("repair: re-read reused shard %d failed", d.Index), rerr)
		}
		if h := checksumBytes(buf); h != cs.Checksum {
			return nil, newError(KindSourceChanged, cs.SourceID,
				fmt.Sprintf("repair: reused shard %d changed between diagnosis and repair: recorded=%s now=%s",
					d.Index, cs.Checksum, h), nil)
		}
	}
	if err := t.resumeSequentialCursors(cp); err != nil {
		return nil, err
	}
	if err := t.executePipeline(ctx, cp, true); err != nil {
		return nil, err
	}
	return t.finishRepair(ctx, cp, out, redone)
}

// finishRepair 执行最终串行等价校验，刷新检查点中的来源指纹并落盘，汇总报告。
func (t *Task) finishRepair(ctx context.Context, cp *Checkpoint, out *RepairReport, redone []int) (*RepairReport, error) {
	// 与 Run 相同的收尾通道：要求输出文件已以读写方式打开（无坏片时也如此）。
	if t.out == nil {
		f, ferr := os.OpenFile(t.cfg.OutputPath, os.O_RDWR|os.O_CREATE, 0o600)
		if ferr != nil {
			if os.IsPermission(ferr) {
				return nil, newError(KindPermission, "", "repair: output open not permitted: "+t.cfg.OutputPath, ferr)
			}
			return nil, newError(KindSourceUnreadable, "", "repair: output open failed: "+t.cfg.OutputPath, ferr)
		}
		t.out = f
	}
	finalHash, err := t.finalVerify(ctx, cp)
	if err != nil {
		return nil, err
	}
	// 成功后才把检查点中的来源指纹推进到当前版本：中途崩溃留下的仍是旧指纹，
	// 下次 Repair 会再次诊断并从同一坏片集合收敛（幂等）。
	for _, st := range t.states {
		cp.UpdateSourceFingerprint(st.ID, st.Fingerprint)
	}
	cp.FinalHash = finalHash
	if err := cp.Save(t.cfg.CheckpointPath); err != nil {
		return nil, err
	}
	out.Repaired = redone
	out.Reused = nil
	for _, d := range out.Doctor.Shards {
		if !containsInt(redone, d.Index) {
			out.Reused = append(out.Reused, d.Index)
		}
	}
	out.FinalHash = finalHash
	t.log.InfoContext(ctx, "repair completed",
		slog.Int("redone", len(out.Repaired)), slog.Int("reused", len(out.Reused)),
		slog.Any("redone_indexes", out.Repaired),
		slog.String("final_sha256", finalHash),
		slog.String("basis", "only diagnosed shards redone; serial-equivalent final verification passed"))
	return out, nil
}

func (r *DoctorReport) statusOf(index int) ShardStatus {
	for _, d := range r.Shards {
		if d.Index == index {
			return d.Status
		}
	}
	return ""
}

func containsInt(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
