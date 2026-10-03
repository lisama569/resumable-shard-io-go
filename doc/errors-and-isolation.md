# 错误分类与失败隔离策略

所有错误都是 `*shardio.Error`，带：

- `Kind`：机器可判别的错误类别；
- `Source`：相关来源 ID（任务级错误为空）；
- `Reason`：人类可读的判定依据（含期望/实际值、分片范围、版本号等）；
- `Cause`：被包装的底层错误（可 `errors.Is/As`）。

同时提供哨兵错误，可用 `errors.Is(err, shardio.ErrXxx)` 或按 Kind 判断。

## 1. 错误分类表

| Kind | 哨兵 | 触发场景（示例） |
| --- | --- | --- |
| `canceled` | `ErrCanceled` | 上游 ctx 被主动取消 |
| `timeout` | — | ctx deadline 到期 |
| `permission_denied` | `ErrPermissionDenied` | 来源只读权限缺失、检查点/输出不可写 |
| `source_changed` | — | 读取期间来源大小/mtime 改变，或收尾指纹与启动不一致 |
| `source_unreadable` | — | 来源无法 stat/open/read、意外 EOF、输出写失败 |
| `checkpoint_corrupt` | `ErrCheckpointCorrupt` | JSON 非法、magic 不符、payload 摘要不符、内部不一致 |
| `checkpoint_version` | `ErrCheckpointVersion` | 检查点版本不受支持 |
| `checkpoint_mismatch` | `ErrCheckpointMismatch` | 与当前任务/来源/分片计划不匹配、完成记录冲突 |
| `checksum_mismatch` | `ErrChecksumMismatch` | 恢复时回读输出摘要不符、最终输出大小/内容不符 |
| `budget_too_small` | `ErrBudgetTooSmall` | 待处理分片比 `InFlightBytes` 还大，最小可行预算为最大分片长度 |
| `source_layout_changed` | `ErrSourceLayoutChanged` | 来源数量/顺序/id/类型/大小变化，分片布局无法与检查点可靠对应，拒绝增量复用 |
| `nothing_to_repair` | `ErrNothingToRepair` | 体检/修复时检查点不存在，没有可处理对象 |
| `invalid_argument` | — | 配置非法、分片越界、顺序源非单调请求等 |

> 三类检查点失败刻意分开：**文件坏了（corrupt）**、**版本不认识（version）**、
> **文件完好但不是这份来源/任务的状态（mismatch）**，调用方可据此选择“删除重建”
> “升级程序”或“拒绝运行并排查来源”。

## 2. 判定依据的可解释性

错误 `Reason` 会打印关键对比信息，例如：

- `source fingerprint mismatch: checkpoint={size=640 hash=ab.. mod=...} current={size=640 hash=cd..}`
  —— 说明大小相同但内容被改写；
- `envelope version 999 unsupported, want 1`；
- `source changed during read: want size=25600 mod=..., got size=25611 mod=...`；
- `resume verify failed for shard 3 [192,256): recorded=.. output=..`。

日志（slog）在 Debug/Info 级别还会打印每个分片的 `index/source/[start,end)/bytes`、
派发与按序交付、检查点 `checkpoint_version` 与每步 `basis`（判定依据）。

## 3. 失败隔离策略

策略是**明确的 fail-fast + 任务级原子边界**，而不是“跳过坏来源继续拼”：

1. **启动阶段任一来源不可用 → 整个任务不启动**。打开来源、计算指纹是读任何数据
   之前的门槛；权限不足、路径不是普通文件等立即返回分类错误，不产生输出。
2. **运行阶段任一来源读取失败或变化 → 取消全任务**。一个分片失败会 cancel 所有
   在途读取，其他来源的在途分片同步终止；已确认分片保留在检查点中，未确认分片
   不会被标记。
3. **绝不产出混合版本**。来源变化时，在途分片在读取前/后做身份复核（size+mtime），
   收尾再做全量 SHA-256 指纹复核；任何一关不过即失败，且失败任务**不写
   `final_hash`**，输出不会被当作成品。
4. **可复现性**：错误带有确定的来源 ID、分片范围与期望/实际值；相同输入与相同
   扰动（如固定时刻改写文件）产生同类错误。并发上限、顺序约束、检查点校验均为
   确定性机制，不依赖时序“碰巧正确”。
5. **恢复隔离**：输出文件被外部篡改时，先于继续读取触发 `checksum_mismatch`，
   阻止在损坏输出上叠加新数据。
6. **体检只读**：`Inspect` 把输出损坏/截断（`output_missing` / `checksum_mismatch`）
   与来源变化作为**报告内容**返回，而不是直接失败或修复；调用方据此决定是否修复。
   修复（`Recover`）默认仍对来源变化保持严格拒绝（`checkpoint_mismatch`），只有显式
   `AllowChangedSources` 且变化仅限同大小内容改写时才增量重算；任何改变分片布局的
   变化返回 `source_layout_changed`。

## 4. 部分进度的去留

- 取消/失败后：检查点保留已确认前缀，输出文件包含对应前缀（可能还有极少量已写入
  但尚未及确认的尾部，恢复时该片会因不在 `done` 集合而在相同偏移被重写覆盖）。
- 再次以**相同**配置 Run：通过校验后只补未确认分片，最终输出经全量大小 + 全量
  SHA-256 + 逐片回读三重确认，与一次不间断串行运行逐字节一致。
