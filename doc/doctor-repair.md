# 崩溃后体检、定向修复、来源变化复用边界与极小预算约定

本章描述面向运维的三类受控能力：

1. **只读体检 `Task.Doctor`**：进程被 `kill -9` / 断电后，在不改动输出与检查点
   的前提下，告诉你“哪些已被认定完成的分片其实对不上、依据是什么”；
2. **定向修复 `Task.Repair`**：体检确认后，只重做有问题的分片，最终结果与
   “全部重跑”逐字节一致；体检与修复均可重复执行且幂等；
3. **极小预算的确定行为**：并发/内存预算被压到比单个处理单元还小时，任务要么
   正常完成（结果与大预算一致、占用不超上限），要么立即给出可区分的失败原因，
   绝不死等。

## 1. 只读体检（Doctor）意味着什么

`Doctor(ctx) (*DoctorReport, error)` 是**纯只读**操作：

- 输出文件以 `O_RDONLY` 打开；检查点只加载、绝不写回；不推进顺序源游标
  （顺序源的审计读取走单独的 pread 通道 `ReadShardAudit`）。
- 只对检查点中**已确认**的分片下结论，未确认分片不在体检范围（它们本来就会重做）。

对每个已确认分片同时比对三方 SHA-256 依据：

| 符号 | 含义 |
| --- | --- |
| R（recorded） | 检查点记录的该片数据校验和 |
| O（output） | 从**输出文件**按“旧分片计划”的全局偏移回读该片得到的校验和 |
| C（current） | 从**当前来源**按 `[start,end)` 重读得到的校验和 |

> 输出偏移按**旧计划**定位，是因为来源大小变化后新旧计划的全局偏移会错开；
> 体检必须能在“来源已经变了”的情况下正确读到旧输出所在的位置。

判定结论：

| status | 条件 | 含义 |
| --- | --- | --- |
| `ok` | O==R 且 C==R | 三方一致，**直接复用**，不重读不重写 |
| `output_damaged` | C==R 但 O!=R（或输出区间缺失/文件被删） | 来源没坏、输出坏了，**重做本片即可** |
| `source_changed` | 来源**同大小**但 C!=R | 本片内容确实受影响，**以当前来源重做本片** |
| `indeterminable` | 来源大小变化 / 前序来源大小变化平移了本片偏移 / 本片在新计划中布局不一致 | **无法逐片证实安全边界，修复必须整体拒绝** |

报告 `DoctorReport` 给出：已确认总数、按状态计数、逐片明细（含三个哈希与
人类可读 `reason`）、发生指纹变化的来源列表，以及 `Repairable` 总开关。

结构性问题（检查点文件损坏、版本不认识、任务/输出/分片大小/来源声明不匹配）
直接返回错误，不出具体检报告；“来源内容变了”属于体检内容而不是结构错误。

## 2. 定向修复（Repair）意味着什么

`Repair(ctx) (*RepairReport, error)` 的固定流程：

1. 做一次与 `Doctor` 完全相同的只读体检；
2. 若存在任何 `indeterminable` 分片：**不改动任何文件**，返回
   `KindRepairNotPossible`（哨兵 `ErrRepairNotPossible`），报告 `Rejected=true`，
   要求整体重跑；
3. 收集 `output_damaged` + `source_changed` 分片，得到“重做集合”；
4. 对**顺序型来源**做后缀扩展：该来源一旦有前序分片要重做，其后的已确认分片
   一并重做（游标不能回退/跳跃，这是顺序设备上能可靠界定的**最小**重做范围；
   随机访问型来源无此约束，坏哪片重做哪片）；
5. 从检查点撤销这些分片的完成记录（同时清空旧 `final_hash`）；
6. 走与普通恢复**完全相同**的流水线：复用片在 `done` 集合中被跳过（好片不重读、
   不覆写），重做片在原全局偏移定点覆盖写入，逐片原子确认检查点；
7. 通过与普通 Run 相同的最终校验（输出总大小 + 全量 SHA-256 + 逐片回读 +
   收尾来源指纹）后，才把检查点中的来源指纹推进为当前版本并写入 `final_hash`。

因此成功修复后的输出与“以当前来源整体重跑”**逐字节一致**；报告 `Repaired` /
`Reused` 两个分片序号列表直接说明“哪些被重做、哪些被复用”。

### 幂等与可重复

- **体检重复执行**：只读，结论稳定，不产生副作用；
- **修复重复执行**：第一次修好后，所有分片都是 `ok`，重做集合为空，第二次是
  空操作（仍跑最终校验以证明成品等价），`Repaired` 为空、全部进入 `Reused`；
- **修复中途再次崩溃**：检查点指纹只在最终校验通过后才推进，坏片撤销记录则已
  逐片落盘；再跑一次 `Repair` 会从同一坏片集合重新收敛，不会把好片改坏；
- 体检与修复在续跑前还会对“被复用片”再做一次回读复核：若体检完成到真正续跑
  之间输出又被改动，本次修复明确失败（`KindSourceChanged` /
  `KindChecksumMismatch`），不带着刚变化的状态继续写。

## 3. 来源变化时的复用与拒绝边界

普通 `Run` 维持严格策略：任一来源指纹变化即 `KindCheckpointMismatch`，不做
自动拼接。`Repair` 在此之上提供**显的增量通道**，边界刻意保守：

### 允许增量（只重做受影响分片，其余复用）

- 来源集合、顺序、声明、**大小**都不变，仅部分字节内容变化（同大小改写）：
  分片边界不变，逐片比较 C 与 R 即可精确指出受影响分片；
- 多来源任务中只有一个来源发生上述变化：只重做该来源的受影响分片，其他来源
  （含顺序型来源）的已确认分片全部复用；
- 顺序型来源受影响：重做“首个受影响分片及其后缀”，保证恢复时游标仍看到连续
  已确认前缀。

### 明确拒绝（不给“看起来成功”的拼接结果）

- **任一来源大小变化**（追加、截断）：分片数量与边界改变，旧输出的全局偏移被
  平移，无法逐片证明哪些字节可安全复用 → 相关分片全部 `indeterminable`，
  `Repair` 返回 `KindRepairNotPossible`；
- 来源数量 / 顺序 / 类型 / 并发声明变化、任务/输出/分片大小变化：结构性错误，
  直接 `KindCheckpointMismatch`；
- 检查点损坏或版本不支持：`KindCheckpointCorrupt` / `KindCheckpointVersion`。

拒绝时输出与检查点保持原样，调用方应改用一次干净的全量重跑（或更换检查点路径）。

## 4. 预算收紧时的行为约定

派发一张分片需要同时满足全局 worker 槽、单源槽与在途字节预算（见
[concurrency.md](concurrency.md)）。其中在途字节预算存在一个“单片不可容纳”的
天然死等点：若预算小于任何一张**待处理**分片，dispatcher 会停在该片前，且没有
任何回收能腾出预算。

为此流水线在派发前做预算预检：

- 若最大的待处理分片长度 > `InFlightBytes`，**立即**返回
  `KindBudgetTooSmall`（哨兵 `ErrBudgetTooSmall`），错误信息带预算值、最大待处理
  分片长度与建议值；在派发任何读取之前失败，不死等、不产生部分读取；
- 只统计**待处理**分片：全部已确认的恢复任务即使预算为 1 字节也是合法的幂等
  空操作（已确认片不占在途预算）；
- 预算 **≥ 单片**（哪怕恰好等于一片、`GlobalWorkers=1`）时任务在有限时间内
  正常串行完成，结果与高预算配置逐字节一致；运行期在途分片数 ≤1、在途字节
  始终不超过预算（背压机制本身保证了占用上界）。

`GlobalWorkers < 1` 规范化为 1；`InFlightBytes <= 0` 使用默认 64 MiB。要在极小
预算下运行，把 `InFlightBytes` 设为不小于 `ShardSize`（以及末片大小）即可，
代价是吞吐降低、内存占用最小。

## 5. 本地复现与验证

```bash
go test ./...                   # 全量（41 个测试）
go test -race -count=3 ./...    # 竞态检测下连跑 3 轮
go vet ./...
go test -v -run 'TestDoctor' ./...
go test -v -run 'TestBudget' ./...
```

新增自动化场景（`doctor_test.go` / `budget_test.go`）：

| 测试 | 场景 |
| --- | --- |
| `TestDoctor_ForceKill_ThenDiagnoseAndSelfHeal` | 强杀后两片输出损坏：体检只读定位 → 修复只重做 2 片 → 体检/修复各自重复执行稳定幂等、好片不被改坏 |
| `TestDoctor_OutputDeleted_FullRepairIdempotent` | 输出整体删除：全部已确认片判损，修复全量重建并收敛 |
| `TestDoctor_RepairReusesGoodShardsWithoutReReading` | 修复流水线不重读好片（读取计数证明复用而非重做） |
| `TestDoctor_SameSizePartialChange_IncrementalRerun` | 同大小部分改写：仅 2 个受影响分片 source_changed 并重做，结果等于以新来源整体重跑，指纹推进到新内容 |
| `TestDoctor_SourceSizeChange_RepairRejected` | 追加改变大小：indeterminable，修复明确拒绝且两文件原样，重复拒绝稳定 |
| `TestDoctor_MultiSource_OnlyAffectedSourceRedone` | 随机+顺序两源中仅一个同大小变化：只重做该源 1 片，顺序源全部复用 |
| `TestDoctor_SequentialSource_SuffixRedo` | 顺序源早期片损坏：体检 1 片损坏，修复扩展为后缀 9 片重做，其他来源 4 片复用，重复修复幂等 |
| `TestDoctor_PartialProgressKill_DamageRepairAndResume` | 部分进度被强杀且含坏片：修复同时完成“坏片重做 + 未完成片续跑”，再跑一次为空操作 |
| `TestDoctor_SameTaskInstanceRepeatedOpensAndClosesSources` | 同一实例连续体检/修复：句柄重开不泄漏，结论稳定 |
| `TestBudget_InFlightBelowOneShard_FailsFast` | 预算 99 < 片长 100：立即 `budget_too_small`，不死等、零读取、结果通道关闭 |
| `TestBudget_ExactlyOneShard_CompletesSequentially` | 预算恰为 1 片、1 worker：串行完成、结果不变、在途并发峰值 ≤1 |
| `TestBudget_TinyBudgetAllCompleted_IsIdempotent` | 极小预算下全完成恢复为空操作 |
| `TestBudget_TaskRunWithSubShardBudget_FailsClearly` | 任务级极小预算在 Run 中给出可区分失败 |

### 手工冒烟（建议开启 Info/Debug 日志）

1. 跑一个多分片任务到完成，`kill -9`（或直接改写输出文件中段、删除输出文件）；
2. 先调用 `Doctor`：确认输出与检查点文件未变化，日志中逐片出现
   `doctor shard diagnosis ... status=ok|output_damaged|... basis=...` 与汇总行
   `doctor report ... repairable=... basis="read-only..."`；
3. 再调用 `Repair`：日志出现 `repair invalidating shard ... status=... basis=...`、
   好片不出现 `shard confirmed`（复用），收尾出现
   `repair completed redone=.. reused=.. redone_indexes=[..] basis=...`；
4. 再次 `Doctor` / `Repair`：应全绿 / 空操作；
5. 把来源同大小覆盖若干字节：仅受影响分片被重做；再向来源追加字节：
   `Doctor` 报 `indeterminable`，`Repair` 返回 `repair_not_possible` 且文件不变；
6. `cmp out.bin <(cat src1 src2 ...)` 必须逐字节一致。
