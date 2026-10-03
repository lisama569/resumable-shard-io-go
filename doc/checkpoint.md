# 检查点格式、版本与兼容规则

## 1. 文件布局

检查点文件为一个 JSON **信封（envelope）**，紧凑编码（不用缩进，避免内嵌字节段
被 JSON 缩进重排）：

```json
{
  "magic": "SHARDIO-CKPT",
  "version": 1,
  "payload": { "...": "..." },
  "sha256": "<hex>"
}
```

- `magic`：固定字符串 `SHARDIO-CKPT`，用于识别文件归属，防止把无关 JSON 文件
  当作检查点加载。
- `version`：信封格式版本（当前为 `1`）。
- `payload`：**紧凑编码**的任务状态 JSON（`json.RawMessage`），字段如下。
- `sha256`：对 `payload` 原始字节计算的 SHA-256（十六进制）。

> 注意：必须用紧凑 `json.Marshal` 序列化信封。若对信封整体使用
> `json.MarshalIndent`，Go 会对内嵌 `json.RawMessage` 做缩进重排，使保存时
> 加载时重算的摘要不一致——本实现的测试已覆盖并锁定该行为。

## 2. payload 字段

| 字段 | 顺序 | 说明 |
| --- | --- | --- |
| `version` | 是 | payload 版本（当前 `1`），必须与信封版本一致 |
| `task_id` | 1 | 任务标识；不同任务不能共用一个检查点 |
| `output` | 2 | 输出文件绝对/给定路径 |
| 分片计划 | 3 | 由 `shard_size` + 每个来源的指纹（size/SHA-256/mtime）派生 |
| `sources[]` | 4 | 来源声明：`id / kind / max_concurrency / fingerprint{size,hash,mod}` |
| `completed` | 5 | `{ "<index>": {index, source_id, start, end, checksum, bytes} }`
| `final_hash` | 6 | 全部完成后对**输出字节流**计算的全量 SHA-256；未完成时为空 |

每片确认记录：

- `index`：全局分片序号（决定合并顺序）；
- `[start,end)`：在**其来源**中的半开字节区间；
- `checksum`：该片数据的 SHA-256；
- `bytes`：片长（必须等于 `end-start`，且与当前分片计划一致）。

## 3. 原子性与崩溃安全

`Checkpoint.Save` 走 `写临时文件 → fsync → close → rename → （尽量）fsync 目录`。
崩溃时只可能出现：旧检查点、或完整新检查点；不会出现半截文件被当作有效状态。
即使出现半截/损坏文件，加载时会被信封 magic 与 payload 摘要检出。

## 4. 恢复时的兼容性校验（严格，不静默跳过）

加载分两步，错误严格区分：

**A. 文件级（不依赖当前来源）—— `LoadCheckpoint`**

1. 不是合法 JSON / 为空 / 无法读取 → `KindCheckpointCorrupt`；
2. `magic != SHARDIO-CKPT` → `KindCheckpointCorrupt`；
3. `payload` 的 SHA-256 不匹配（截断、篡改、写坏）→ `KindCheckpointCorrupt`；
4. 信封 `version` 或 payload `version` 不是本实现支持的版本 →
   `KindCheckpointVersion`（**先**判版本，再解 payload）；
5. 内部一致性（缺 task_id、来源 id 重复、未知 kind、完成记录引用未知来源、
   区间/长度非法等）→ `KindCheckpointCorrupt`。

**B. 与当前任务/来源匹配 —— `Checkpoint.ValidateAgainst`**

依次比对并在首个不一致处给出带明细的 `KindCheckpointMismatch`：

1. `task_id` 不同；
2. `output` 路径不同；
3. `shard_size` 不同；
4. 来源数量不同；
5. 第 i 个来源的 `id / kind / max_concurrency` 不同；
6. 第 i 个来源的**指纹**不同（size、全量 SHA-256、mtime 三者全等才算一致；
   “同大小但内容改写”也能识别）。

**C. 与当前分片计划的布局一致性**

完成记录中的每个分片必须落在当前计划里：`index / source_id / [start,end) / bytes`
全部一致，否则 `KindCheckpointMismatch`。

**D. 输出文件回读校验**

检查点通过后，对每条已确认记录，从输出文件对应全局偏移回读该片，复核长度与
SHA-256。输出文件被外部删除、截断、篡改时：

- 文件太小装不下已确认区间 → `KindCheckpointMismatch`；
- 回读摘要不一致 → `KindChecksumMismatch`。

这保证“检查点完好但输出坏了”不会被误认为已完成。

## 5. 幂等语义

- **重复提交同一完成记录**（所有字段全等）：`MarkCompleted` 静默接受，不重复计数；
- **同 index 重复提交但内容冲突**（不同校验和/区间/来源）：返回
  `KindCheckpointMismatch`，绝不以后者静默覆盖前者；
- **乱序完成**（先确认 index=5，再确认 index=1）：合法，map 存储、独立有效；
  交付顺序仍由流水线 collector 严格保证升序；
- **重启后**：已确认分片进入 `done` 集合被跳过（随机源直接跳过；顺序源通过
  `ResumeAt` 把游标推进过已确认的连续前缀，语义等同“卷过”），不重复处理。

## 6. 崩溃后的体检与定向修复如何使用检查点

- `Doctor`（只读体检）**只加载**检查点，不写回；它用检查点中的 `sources[]`
  重建“旧分片计划”来定位输出中的已确认字节，用 `completed[].checksum` 作为
  三方比对中的记录值 R。
- `Repair`（定向修复）对被判定损坏/过时的分片调用 `ForgetCompleted(index)`
  撤销完成记录；该操作同时清空 `final_hash`，因为整体等价性需要重新证明。
  撤销记录随普通恢复流水线逐片重新确认（仍走临时文件 + fsync + rename），
  故修复中途再次崩溃也只会留下“少若干确认片”的状态，下次修复从同一坏片
  集合收敛。
- 仅当最终串行等价校验通过后，`Repair` 才用 `UpdateSourceFingerprint` 把
  检查点中的来源指纹推进为当前版本并写入新的 `final_hash`；失败/拒绝路径
  保留旧指纹，不产生“指纹已更新但内容没修完”的状态。
- 影响范围无法逐片证实时（如来源大小变化），`Repair` 不调用任何写操作，
  直接返回 `KindRepairNotPossible`。

## 7. 版本演进规则

当前仅支持 `version=1`。未来升级应：

- 新增**可选字段**保持向后兼容时，维持同一版本并用零值/默认处理；
- 改变字段含义、分片边界算法或摘要算法时，提升版本号；旧版本文件必须返回
  `KindCheckpointVersion` 并说明“checkpoint version X unsupported, want Y”，
  不得尝试猜测迁移。
