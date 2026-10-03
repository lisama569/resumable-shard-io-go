# 崩溃后体检、自愈与来源变化增量重算

本章描述三类运维场景下的确定性行为：进程被 `kill -9` / 断电后的**只读体检**与
**定向修复**；恢复时**来源内容发生变化**的影响范围界定与增量重算；以及预算被压到
极小时的行为约定。

## 1. 只读体检 `Task.Inspect(ctx) (*InspectionReport, error)`

体检回答两个问题，并且**绝不写输出、绝不写检查点、绝不推进顺序源游标**：

1. 检查点里“已确认完成”的分片，输出文件对应区间的真实字节是否仍与记录一致；
2. 各来源当前指纹（size + 全量 SHA-256 + mtime）是否仍是检查点记录的那一份。

判定依据逐片/逐源给出（`ShardHealth` / `SourceHealth`）：

| 结论 | 含义 | 依据 |
| --- | --- | --- |
| `ok` | 该片可复用 | 从输出的全局偏移回读该片，SHA-256 与检查点记录一致 |
| `output_missing` | 输出文件不存在/被截断，装不下该片 | 文件缺失或 `offset+bytes > file_size` |
| `checksum_mismatch` | 区间存在但内容被改写/损坏 | 回读 SHA-256 与记录不同（报告同时给出两者作为证据） |

- 输出文件整体缺失时，所有已确认片报 `output_missing`，而不是报错；
- 单个来源当前指纹不可读（如权限不足）只影响该来源的结论，不影响输出体检；
- 只有检查点缺失、损坏、版本不兼容或不属于本任务（task_id/output/shard_size
  对不上）时，体检才**返回错误**：
  - 检查点不存在 → `KindNothingToRepair`（哨兵 `ErrNothingToRepair`）。

`InspectionReport.Healthy()` 在“无坏片且无来源变化”时为 true。体检可以安全重复
执行任意次，结果只取决于磁盘当前状态。

## 2. 定向修复 `Task.Recover(ctx, RecoverOptions) (*RecoverReport, error)`

修复在体检的同一套判定上执行，处理顺序为：

1. 加载并严格校验检查点（与体检相同的硬边界）；
2. 用**检查点冻结的分片计划**重建全局偏移（输出布局由它定义）；
3. 回读输出，找出所有坏片；
4. （可选，见第 3 节）界定来源变化的影响范围；
5. **先**把“需要重做”的决定原子写入检查点（失效对应完成记录），**再**触碰输出；
6. 复用正常 Run 的流水线：只重做失效分片，在原全局偏移重写并逐片确认；
7. 最终做输出大小 + 全量 SHA-256 + 逐片回读的串行等价校验。

报告区分三类分片：`Reused`（已确认且健康、原样复用）、`Repaired`（已确认但被判定
有问题、本次重做）、`Pending`（上次中断前从未确认、本次补齐）。

**只重做有问题的部分**：健康分片既不重新读取来源，也不重写输出。修复结果与
“删除输出与检查点后全部重跑”逐字节一致——最终都要通过同一道串行等价校验。

### 幂等与稳定性

- 修复重复执行：第二次体检全绿，`Repaired` 为空、全部进入 `Reused`，输出字节不变；
- 修复不会把原本没问题的部分改坏：未失效分片不经过写路径；
- 输出文件整体丢失也可修复：按检查点计划逐片重建；
- 修复过程中再次崩溃：已重新确认的片落盘，未确认的片下次继续，语义与普通恢复一致。

## 3. 来源变化时的复用边界与拒绝边界

恢复时若发现来源指纹与检查点不同，需要区分“能可靠界定影响范围”与“不能”：

### 3.1 默认：拒绝拼接（安全优先）

不带选项调用 `Recover` 时，任何来源指纹变化都返回 `KindCheckpointMismatch`，
绝不把旧来源的已完成字节与新来源的字节拼成一份看起来成功的结果。普通 `Run`
同样保持严格匹配（既有行为不变）。

### 3.2 允许同大小内容改写：逐片界定、只重算受影响部分

`RecoverOptions{AllowChangedSources: true}` 放开**且仅放开**一种变化：

- 来源**数量、顺序、id、类型、大小全部不变**，只有内容被同长度改写。

界定方法：对变化来源的每个**已确认**分片，用独立只读句柄从**当前**来源回算该片
SHA-256，与检查点记录逐片比对——

- 摘要一致 ⇒ 该片字节未受影响，**复用**（即使同文件其他位置被改写）；
- 摘要不同 ⇒ 受影响，重做。

对**随机访问型**来源，只重做受影响的那些分片（可以不连续）。对**顺序型**来源，
由于游标只能单调推进、不能回退跳过，从其**第一个受影响分片起的已确认后缀**全部
重做；之前的分片继续复用。修复完成后检查点中的来源指纹同步为当前值，后续普通
`Run`/`Inspect` 不再报变化。

影响范围界定使用独立只读文件句柄，不推进任务实际顺序源的游标；因此顺序源仍能
正确 `ResumeAt` 到复用前缀。

### 3.3 无法可靠界定时明确失败

以下变化会改变分片数量或全局序号映射，无法与既有输出偏移可靠对应，一律返回
`KindSourceLayoutChanged`（哨兵 `ErrSourceLayoutChanged`），不做任何拼接：

- 来源数量增减、顺序变化、id 变化、类型（random/sequential）变化；
- 任一来源**大小**变化（增长、缩短都会使分片边界/数量变化）；
- 当前来源指纹无法取得（无法判断影响范围）。

判定原则：**能逐字节证明不受影响才复用；证明不了就失败，不猜。**

## 4. 极小预算下的行为约定

- `InFlightBytes < 某个待处理分片长度`：该分片即使在系统完全空闲时也永远无法满足
  “在途字节 + 片长 ≤ 预算”，流水线在派发前快速失败，返回
  `KindBudgetTooSmall`（哨兵 `ErrBudgetTooSmall`），错误中带分片长度与预算值，
  **不会无限等待**。
- `InFlightBytes == 最大分片长度`：这是“可行的最紧预算”，一次只允许一片在途，
  任务退化为串行处理但**正常完成**，输出与预算充足时逐字节一致。
- 已全部确认、无需派发任何分片的恢复：预算多小都正常成功（预算只约束在途数据）。
- 运行期占用上界不变：在途数据始终 ≤ `InFlightBytes`，全局在途分片 ≤ `GlobalWorkers`，
  单源在途分片 ≤ 该源 `MaxConcurrency`（顺序源恒为 1）。`GlobalWorkers` 规范化为 ≥1。

## 5. 日志可观测性

体检与修复的每一步判定都打 Info 日志并带 `basis`（判定依据），关键行：

- `healthcheck shard ... verdict=ok|output_missing|checksum_mismatch basis=...`
- `healthcheck source ... matches=true|false basis=...`
- `healthcheck complete ... damaged_shards=.. changed_sources=.. healthy=.. basis=read-only...`
- `repair: shard will be redone ... reason=.. basis=..`
- `repair: affected by source change ... recorded=.. current=.. basis=recomputed from current source content`
- `repair: sequential source redo suffix ... from_index=.. basis=sequential cursor cannot rewind`
- `repair plan ... reused=.. redone=.. pending=.. redo_indexes=[..] changed_sources=[..]`
- `repair completed ... final_sha256=.. basis=...`

复用了哪些（`reused`）、重做了哪些（`redone`/`pending`，精确到分片序号）、为什么
这样判定（输出摘要不符 / 来源回算摘要不符 / 顺序源后缀）都可直接从日志读出。

## 6. 本地复现与验证

```bash
go test -run 'TestInspect|TestRecover' -v ./...   # 体检与自愈场景
go test -run 'Budget' -v ./...                    # 极小预算场景
go test -race -count=3 ./...                      # 竞态下多轮稳定性
```

手工复现“kill -9 → 体检 → 修复”：

1. 正常跑到一半 `kill -9 <pid>`（或用可取消任务跑到部分确认后退出）；
2. （可选）用 `dd`/编辑器改坏输出文件中某几个字节，或直接 `rm` 输出文件；
3. 调用 `Inspect`：观察 `verdict` 与 `basis`，确认输出、检查点文件字节未变化；
4. 调用 `Recover(RecoverOptions{})`：观察 `repair plan` 中被重做与被复用的分片；
5. 再次 `Inspect` 必须全绿；再次 `Recover` 必须 `redone=0`；
6. `sha256sum out.bin` 与一份干净全量重跑的输出一致。

手工复现“来源变化增量重算”：

1. 完成任务后，用**相同字节数**覆盖来源文件的某个区间（如
   `dd if=<(printf 'BBBB...') of=src.bin bs=1 seek=256 conv=notrunc`）；
2. `Inspect` 报告该来源 `matches=false`；直接 `Recover` 被拒（`checkpoint_mismatch`）；
3. `Recover{AllowChangedSources: true}`：只有覆盖区间所在分片被重做，其余复用；
4. 把来源文件**追加/截断**改变大小后再修复：得到 `source_layout_changed`，
   输出与检查点不被改动。
