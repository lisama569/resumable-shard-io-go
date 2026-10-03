# 本地验证方法

## 1. 命令

```bash
go version                      # 需要 Go 1.25+（在 Go 1.26 上开发并验证）
go test ./...                   # 49 个测试
go test -race -count=3 ./...    # 竞态检测下连跑 3 轮（约 14s）
go vet ./...
go test -list '.*'              # 列出全部测试名
# 聚焦本轮新增能力：
go test -run 'TestInspect|TestRecover' -v ./...
go test -run 'Budget' -v ./...
```

测试分三层：内存管线、检查点、真实磁盘任务集成（含体检/自愈）。
带 `-v` 运行时每个用例都会打印 `=== RUN` / `--- PASS`，可直接看出覆盖了哪些
场景；体检与修复内部还会打 `healthcheck ...` / `repair plan ...` 等 Info 日志
（测试默认静默，需要时把 Logger 接到 stderr，见第 6 节）。

## 2. 内存管线层（无需磁盘，12 个）

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
| `TestPipeline_BudgetSmallerThanShardFailsFast` | 预算小于单片 → `budget_too_small`，无读取开始、不卡死 |
| `TestPipeline_BudgetExactlyShardSizeStillCompletes` | 预算恰为单片大小：串行完成，结果与充足预算一致 |
| `TestPipeline_OneWorkerMinimalBudgetSequentialSource` | 顺序源 + 单 worker + 最紧预算仍有限时间完成 |

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

## 4. 任务集成层（真实磁盘，29 个）

### 4a. 既有恢复/校验（15 个，无回归）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestTask_EndToEnd_MixedKinds` | 两类来源端到端合并，结果等于顺序拼接 |
| `TestTask_CancelThenResumeTwice` | 取消→恢复→再取消→恢复完成；进度严格递增、不重不漏 |
| `TestTask_RerunAfterCompletionIsIdempotent` | 完成后重跑全部跳过、结果不变 |
| `TestTask_SourceChangedBetweenRuns` | 两次运行间同大小改写内容 → mismatch（靠哈希而非大小） |
| `TestTask_SourceChangedDuringRun` | 单次运行中来源被追加 → `source_changed` |
| `TestTask_CorruptCheckpointFailsClearly` | 物理损坏检查点 → corrupt，不静默重建 |
| `TestTask_UnsupportedCheckpointVersion` | 版本被改 → version（若先破坏摘要则 corrupt） |
| `TestTask_PermissionDenied_Source` | 来源 `chmod 000` → `permission_denied`（root 下自动 skip） |
| `TestTask_SequentialSourceResume` | 顺序源恢复时游标越过已确认连续前缀 |
| `TestTask_FailureIsolationOneSourceChanges` | 多源中仅一个变化：错误标识该源、不记 final_hash、不产混合版本 |
| `TestMergeSerial_ReferenceEquivalence` | 管道输出、参考串行实现、最终哈希三者一致 |
| `TestTask_BudgetSmallerThanShardFailsClearly` | 任务级：预算小于单片 → 有限时间内 `budget_too_small` |
| `TestTask_BudgetExactlyShardSizeProducesSameResult` | 任务级：单 worker + 预算等于单片，正常完成且结果一致 |
| `TestTask_CompletedTaskTinyBudgetStillSucceeds` | 全完成恢复在任意小预算下都成功（无需派发） |

### 4b. 体检（只读，5 个）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestInspect_HealthyTask` | 刚完成任务体检全绿；输出与检查点字节零改动（只读证明） |
| `TestInspect_DetectsCorruptedAndTruncatedOutput` | 改坏单片区 → `checksum_mismatch` 且携带 recorded/output 双摘要；再截断 → 尾部 `output_missing`，未触及片仍 ok |
| `TestInspect_OutputFileDeleted` | 输出整体删除：所有已确认片报 `output_missing`，不返回错误 |
| `TestInspect_ChangedSameSizeSource` | 同大小改写来源：输出片仍 ok，但来源报 `matches=false` 并给出指纹依据 |
| `TestInspect_NoCheckpoint` | 无检查点 → `nothing_to_repair`（`errors.Is(ErrNothingToRepair)`） |

### 4c. 自愈与增量重算（9 个）

| 测试 | 覆盖路径 |
| --- | --- |
| `TestRecover_CorruptedOutputRedoneOnly` | 体检→修复闭环：只重做坏片、8 片复用；结果=全量重跑；修复后体检全绿 |
| `TestRecover_DeletedOutputRebuiltAndIdempotent` | 输出删除后整体重建；第二次修复 `redone=0`、输出字节不变（幂等） |
| `TestRecover_TruncatedOutputSequentialSource` | 顺序源输出截断：只重做缺失后缀，游标正确推进 |
| `TestRecover_SameSizeSourceChangeIncremental` | 同大小改写随机源：仅受影响分片重做（来源实际只读 1 次），未选 opt-in 时拒绝；修复后指纹同步、严格 Run 通过 |
| `TestRecover_SequentialSameSizeChangeRedoesSuffix` | 同大小改写顺序源：首个受影响分片起的已确认后缀重做，前缀复用 |
| `TestRecover_MultiSourceChangeIsolatesUnaffected` | 多源中只改一个随机源的一片：其他来源（含顺序源）分片全部复用，结果=新r+旧s |
| `TestRecover_SourceSizeGrowRefused` | 来源追加变大 → `source_layout_changed`（`errors.Is`），不拼接 |
| `TestRecover_AfterHardKillSimulated` | 中途取消（kill -9 等价）+ 输出再损坏：坏片重做、未完成片补齐（report 三类分片合计=总数），结果=全量重跑 |
| `TestRecover_ThenInspectTwiceStable` | 修复/体检连做两轮稳定、输出不漂移 |

## 5. 确定性地模拟“运行中来源变化”

直接“起任务后立刻改文件再取消”存在竞态（任务可能已经跑完，或取消先于变化）。
测试改为注入一个 **stalling 来源**：前 N 次读取正常，之后在读之前阻塞；测试用
`atomic` 计数等到确有分片阻塞（即确定存在未完成读取）后再改写文件、最后放行。
于是读取前/后的身份复核必然命中来源变化。该构造在 `-race -count=3` 下稳定。

## 6. 日志可观测性

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
- `task completed ... final_sha256=.. basis=serial-equivalent merge verified`

## 7. 手工冒烟

准备两个文件，用示例配置跑一次（应成功），`kill` 在中途后再跑一次（应从检查点
恢复并成功），随后：

1. `chmod 000` 来源 → 期望 `permission_denied`；
2. 同大小覆盖来源内容 → 期望 `checkpoint_mismatch`；
3. 截断/翻改检查点 → 期望 `checkpoint_corrupt`；
4. 比对输出与 `cat source1 source2` 必须逐字节一致。

体检/自愈与增量重算的手工复现步骤见 [doctor.md](doctor.md) 第 6 节。
