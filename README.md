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
- **崩溃后只读体检与定向自愈**：`Inspect` 只读回读输出逐片比对、报告坏片与来源变化
  依据，不触碰任何输出/进度；`Recover` 只重做有问题的部分，结果与全量重跑一致，
  重复执行幂等。
- **来源变化增量重算**：同大小内容改写可逐片界定影响范围，仅重算受影响分片；
  大小/数量/类型变化等无法可靠界定时明确 `source_layout_changed`，拒绝拼接。
- **极小预算确定性行为**：在途预算小于单片时快速失败（`budget_too_small`）而非卡死；
  预算恰好等于单片时退化为串行并正常完成，占用不超上限。
- **两类本地来源协同**：`RandomAccess`（pread，可受限并发）与 `Sequential`
  （单调游标，单源串行）。
- **干净取消**：取消/超时取消全部在途读取，无 goroutine 泄漏、无未完成处理。

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

进程被 `kill -9` / 断电后，先做只读体检、确认依据，再定向修复：

```go
// 只读：不写输出、不写检查点，报告哪些已确认分片对不上、依据是什么
report, err := task.Inspect(ctx)
if !report.Healthy() {
    // 只重做有问题的部分；健康分片原样复用；结果与全量重跑一致
    _, err = task.Recover(ctx, shardio.RecoverOptions{})
}
// 若来源是“同大小内容改写”，可显式允许增量重算（只重算受影响分片）；
// 大小/数量/类型变化会得到 KindSourceLayoutChanged，拒绝拼接新旧内容。
_, err = task.Recover(ctx, shardio.RecoverOptions{AllowChangedSources: true})
```

## 深入文档

- [doc/checkpoint.md](doc/checkpoint.md)：检查点文件格式、版本与兼容规则、恢复判定。
- [doc/concurrency.md](doc/concurrency.md)：并发模型、单源上限、背压与内存上界、取消语义、极小预算。
- [doc/doctor.md](doc/doctor.md)：崩溃后体检与自愈、来源变化的复用/拒绝边界、复现步骤。
- [doc/errors-and-isolation.md](doc/errors-and-isolation.md)：错误分类表、失败隔离策略。
- [doc/verification.md](doc/verification.md)：本地验证方法（竞态、场景覆盖、日志）。

## 本地验证

```bash
go test ./...                 # 功能 + 集成
go test -race -count=3 ./...  # 竞态检测下多轮稳定性
go vet ./...
```
