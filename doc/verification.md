# 本地验证方法

## 1. 命令

```bash
go version                      # 需要 Go 1.25+（在 Go 1.26 上开发并验证）
go test ./...                   # 41 个测试
go test -race -count=3 ./...    # 竞态检测下连跑 3 轮（约 13s）
go vet ./...
go test -list '.*'              # 列出全部测试名
go test -v -run 'TestDoctor' ./...   # 仅体检/修复/来源变化场景
go test -v -run 'TestBudget' ./...   # 仅极小预算场景
```

测试分三层：内存管线、检查点、真实磁盘任务集成。

## 2. 内存管线层（无需磁盘，9 个）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestPipeline_OrderedOutputEqualsSerial` | 高并发下结果仍严格按序，合并等于顺序拼接 |
| `TestPipeline_ConcurrencyCaps` | 单源实际并发不超过配置上限（读前阻塞探测峰值） |
| `TestPipeline_BackpressureBoundsInFlight` | 慢消费时开始读取的分片数被在途字节预算封住 |
| `TestPipeline_SequentialMonotonicDispatch` | 顺序源始终收到单调偏移请求 |
| `TestPipeline_SourceFailureCancelsEverything` | 一片失败即取消全部在途读取 |
| `TestPipeline_CancelStopsAllWork` | 主动取消后无任何遗留 active 读取，返回 `canceled` |
| `TestPipeline_TimeoutClassified` | 超时返回 `timeout` |
| `TestPipeline_SkipCompletedShards` | 全完成计划一次读取都不触发（顺序源恢复等价） |
| `TestPipeline_MixedSourcesInOneRun` | 随机 + 顺序两源在同一任务中协同、合并正确 |

## 3. 检查点层（8 个）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestCheckpoint_RoundTrip` | 保存/加载往返一致 |
| `TestCheckpoint_MissingFileReturnsNil` | 无检查点时返回 `(nil,nil)`，等同首次运行 |
| `TestCheckpoint_CorruptVariants` | 非 JSON/截断/翻字节/bad magic 全部判为 corrupt |
| `TestCheckpoint_VersionIncompatible` | 未来版本判为 version 且 `errors.Is(ErrCheckpointVersion)` |
| `TestCheckpoint_ValidateAgainst` | task_id/output/shard_size/来源数/指纹不匹配均判 mismatch |
| `TestCheckpoint_MarkCompletedIdempotent` | 相同重复幂等；冲突重复明确失败 |
| `TestCheckpoint_OutOfOrderCompletion` | 乱序完成可保存、可恢复 |
| `TestCheckpoint_AtomicSaveLeavesNoTmp` | 多次原子保存不残留临时文件 |

## 4. 任务集成层（真实磁盘，11 个，含 1 个参考等价用例）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestTask_EndToEnd_MixedKinds` | 两类来源端到端合并，结果等于顺序拼接 |
| `TestTask_CancelThenResumeTwice` | 取消→恢复→再取消→恢复完成；进度严格递增、不重不漏 |
| `TestTask_RerunAfterCompletionIsIdempotent` | 完成后重跑全部跳过、结果不变 |
| `TestTask_SourceChangedBetweenRuns` | 两次运行间同大小改写内容 → mismatch（靠哈希而非大小） |
| `TestTask_SourceChangedDuringRun` | 单次运行中来源被追加 → `source_changed` |
| `TestTask_OutputTamperedBeforeResume` | 恢复前篡改已确认输出 → `checksum_mismatch`，不续写坏数据 |
| `TestTask_CorruptCheckpointFailsClearly` | 物理损坏检查点 → corrupt，不静默重建 |
| `TestTask_UnsupportedCheckpointVersion` | 版本被改 → version（若先破坏摘要则 corrupt） |
| `TestTask_PermissionDenied_Source` | 来源 `chmod 000` → `permission_denied`（root 下自动 skip） |
| `TestTask_SequentialSourceResume` | 顺序源恢复时游标越过已确认连续前缀 |
| `TestTask_FailureIsolationOneSourceChanges` | 多源中仅一个变化：错误标识该源、不记 final_hash、不产混合版本 |
| `TestMergeSerial_ReferenceEquivalence` | 管道输出、参考串行实现、最终哈希三者一致 |

## 5. 体检/修复与极小预算层（真实磁盘 + 内存管线，13 个）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestDoctor_ForceKill_ThenDiagnoseAndSelfHeal` | 强杀后输出坏两片：只读体检定位（且两文件逐字节不变）→ 修复只重做坏片 → 体检/修复重复执行稳定、好片不坏、终态哈希正确 |
| `TestDoctor_OutputDeleted_FullRepairIdempotent` | 输出被删：全部判 `output_damaged`，全量重建 |
| `TestDoctor_RepairReusesGoodShardsWithoutReReading` | 修复流水线读取计数证明好片不被重读（复用而非重做） |
| `TestDoctor_SameSizePartialChange_IncrementalRerun` | 同大小部分改写：仅受影响片 `source_changed` 并重做，结果等于以新来源整体重跑，指纹推进 |
| `TestDoctor_SourceSizeChange_RepairRejected` | 追加改大小：`indeterminable` → `repair_not_possible`，拒绝时两文件不变且可重复拒绝 |
| `TestDoctor_MultiSource_OnlyAffectedSourceRedone` | 随机+顺序两源中仅一个同大小变化：只重做该源分片，顺序源全部复用 |
| `TestDoctor_SequentialSource_SuffixRedo` | 顺序源早期片损坏：修复扩展为后缀重做，其他来源复用，重复修复幂等 |
| `TestDoctor_PartialProgressKill_DamageRepairAndResume` | 部分进度强杀且含坏片：修复同时完成坏片重做与未完成片续跑 |
| `TestDoctor_SameTaskInstanceRepeatedOpensAndClosesSources` | 同一 Task 实例连续多次体检/修复：来源与输出句柄重开不泄漏、互不干扰 |
| `TestBudget_InFlightBelowOneShard_FailsFast` | 预算 < 单片：立即 `budget_too_small`，不死等、零读取、通道关闭 |
| `TestBudget_ExactlyOneShard_CompletesSequentially` | 预算 = 单片、1 worker：串行完成、结果一致、在途峰值 ≤1 |
| `TestBudget_TinyBudgetAllCompleted_IsIdempotent` | 极小预算下全完成恢复为空操作 |
| `TestBudget_TaskRunWithSubShardBudget_FailsClearly` | 任务级极小预算给出可区分失败 |

## 6. 确定性地模拟“运行中来源变化”

直接“起任务后立刻改文件再取消”存在竞态（任务可能已经跑完，或取消先于变化）。
测试改为注入一个 **stalling 来源**：前 N 次读取正常，之后在读之前阻塞；测试用
`atomic` 计数等到确有分片阻塞（即确定存在未完成读取）后再改写文件、最后放行。
于是读取前/后的身份复核必然命中来源变化。该构造在 `-race -count=3` 下稳定。

## 7. 日志可观测性

默认使用 `slog`（测试用 `io.Discard` 静默）。排查时把级别调到 Debug：

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
cfg.Logger = logger
cfg.Pipeline.Logger = logger
```

关键日志行：

- `checkpoint created/loaded ... checkpoint_version=.. basis=..`
- `shard dispatched ... start=.. end=.. bytes=.. in_flight_bytes=..`
- `shard delivered in order ... checksum=..`
- `backpressure: waiting for in-flight budget` / `waiting for source slot`
- `shard confirmed ... range_start=.. range_end=.. basis=read+written+checkpointed`
- `resume verify ok ... output_offset=.. checksum=..`
- `doctor shard diagnosis ... status=ok|output_damaged|source_changed|indeterminable recorded=.. output_hash=.. current_hash=.. basis=..`
- `doctor report ... ok=.. output_damaged=.. source_changed=.. indeterminable=.. repairable=.. changed_sources=.. basis="read-only..."`
- `repair invalidating shard ... status=.. basis=..` / `repair redo-set expanded ... basis="sequential source..."`
- `repair no-op ... basis="all completed shards diagnosed ok"`
- `repair rejected ... basis=..`（无法界定影响范围时）
- `repair completed redone=.. reused=.. redone_indexes=[..] final_sha256=.. basis="only diagnosed shards redone..."`
- `pipeline rejected ... in_flight_limit=.. largest_pending_shard=.. basis="budget smaller than one pending shard..."`
- `task completed ... final_sha256=.. basis=serial-equivalent merge verified`

## 8. 手工冒烟

准备两个文件，用示例配置跑一次（应成功），`kill` 在中途后再跑一次（应从检查点
恢复并成功），随后：

1. `chmod 000` 来源 → 期望 `permission_denied`；
2. 同大小覆盖来源内容 → 期望 `checkpoint_mismatch`；
3. 截断/翻改检查点 → 期望 `checkpoint_corrupt`；
4. 比对输出与 `cat source1 source2` 必须逐字节一致。
