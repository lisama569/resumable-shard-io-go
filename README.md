# resumable-shard-io-go

可恢复的**本地并行分片读取与校验流水线**。多个能力不同的本地来源（随机访问型 +
顺序型）在同一任务中受限并发读取，结果按分片顺序合并；在**并发、来源失败、主动
取消与进程重启**场景下，输出始终确定、可解释，并且与“按来源顺序、按偏移串行
读取”的参考实现逐字节等价。

## 特性概览

- **分片 + 受限并发**：来源按固定 `ShardSize` 切成全局有序分片；受全局 worker
  数、**单源并发上限**、在途字节预算三重约束。
- **有序合并**：并发读取、按 `Shard.Index` 严格升序交付，输出与串行读取一致。
- **内存有界（背压）**：未按序交付的数据总量被 `InFlightBytes` 封顶；即使消费端
  很慢，内存占用也有明确上界。
- **可恢复检查点**：每片“读取 → 写输出 → fsync 式原子落盘检查点”；重启后已
  确认分片不重读、不丢失。重复提交 / 乱序完成均幂等。
- **检查点严格校验**：损坏、版本不兼容、与当前来源不匹配分别给出**可区分**错误，
  绝不静默跳过。
- **来源变化检测**：启动与收尾做全量 SHA-256 指纹，读取热路径做 size+mtime 复核；
  读取期间来源被替换/追加/改写一律失败，绝不产出混合版本。
- **两类本地来源协同**：`RandomAccess`（pread，可受限并发）与 `Sequential`
  （单调游标，单源串行）。
- **干净取消**：取消/超时取消全部在途读取，无 goroutine 泄漏、无未完成处理。
- **崩溃后只读体检（Doctor）**：`kill -9`/断电后，在**不改动输出与检查点**的
  前提下，逐片比对“检查点记录 / 输出回读 / 当前来源重读”三方 SHA-256，报告
  哪些已确认分片对不上（输出损坏 / 来源变化 / 无法界定）及判定依据。
- **定向自愈（Repair）**：体检后**只重做有问题的分片**，好片不重读、不覆写，
  最终结果与整体重跑逐字节一致；体检与修复均可重复执行且幂等。来源大小变化等
  无法逐片证实安全边界的情况，修复**明确拒绝**（`repair_not_possible`），
  绝不拼接新旧内容。
- **极小预算确定行为**：在途字节预算小于任何待处理分片时，立即以
  `budget_too_small` 失败（不死等、零读取）；预算不小于单片时正常完成，
  结果与大预算一致且占用不超上限。

## 快速开始

```go
task, _ := shardio.NewTask(shardio.TaskConfig{
    TaskID:         "job-2026-09-24",
    CheckpointPath: "/data/job.ckpt",
    OutputPath:     "/data/out.bin",
    ShardSize:      4 << 20, // 4 MiB
    SourceSpecs: []shardio.SourceSpec{
        {ID: "src-random", Path: "/data/a.log", Kind: shardio.RandomAccess, MaxConcurrency: 4},
        {ID: "src-seq",    Path: "/data/b.log", Kind: shardio.Sequential},
    },
    Pipeline: shardio.PipelineConfig{GlobalWorkers: 8, InFlightBytes: 64 << 20},
})
if err := task.Run(ctx); err != nil {
    // err 是 *shardio.Error，可用 Kind / errors.Is 精确分类处理
}
```

中断后用**完全相同**的 `TaskID / 输出路径 / 分片大小 / 来源集合`再次 `Run` 即从
检查点恢复；来源发生任何变化都会得到 `KindCheckpointMismatch` 而非错误结果。

进程被强制杀掉、怀疑输出或来源已被改动时，先用**只读体检**再决定是否修复：

```go
// 只读：不改动输出与检查点
report, err := task.Doctor(ctx)
// report.Shards[i].Status: ok / output_damaged / source_changed / indeterminable
// report.Repairable=false 表示影响范围无法可靠界定，应整体重跑

// 定向自愈：只重做报告中有问题的分片；无问题时是幂等空操作
rreport, err := task.Repair(ctx)
// rreport.Repaired / rreport.Reused 列出重做与复用的分片序号
```

详见 [doc/doctor-repair.md](doc/doctor-repair.md)。

## 深入文档

- [doc/checkpoint.md](doc/checkpoint.md)：检查点文件格式、版本与兼容规则、恢复判定。
- [doc/concurrency.md](doc/concurrency.md)：并发模型、单源上限、背压与内存上界、取消语义。
- [doc/doctor-repair.md](doc/doctor-repair.md)：只读体检、定向修复、来源变化复用/拒绝边界、极小预算约定。
- [doc/errors-and-isolation.md](doc/errors-and-isolation.md)：错误分类表、失败隔离策略。
- [doc/verification.md](doc/verification.md)：本地验证方法（竞态、场景覆盖、日志）。

## 本地验证

```bash
go test ./...                 # 功能 + 集成（41 个测试）
go test -race -count=3 ./...  # 竞态检测下多轮稳定性
go vet ./...
```
