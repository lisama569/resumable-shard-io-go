# 并发、背压与取消语义

## 1. 三重并发约束

设一个来源的并发上限为 C_s（随机访问型取配置的 `MaxConcurrency`，顺序型恒为 1），
全局 worker 数为 W（`GlobalWorkers`），在途字节预算为 B（`InFlightBytes`）。

一张分片要被派发，必须同时满足：

1. **全局槽**：已派发但尚未完成交付的分片数 < W（由容量为 W 的缓冲 job 通道天然实现）；
2. **单源槽**：该来源已派发但尚未按序回收的分片数 < C_s（由 dispatcher 单线程
   维护的 `active[sid]` 计数实现，无锁）；
3. **字节预算**：在途数据字节数 + 本片长度 ≤ B。

三个条件都满足才派发给 worker；不满足时 dispatcher 停在该分片前等待。

## 2. 有序交付与单源串行

- **collector** 维护全局下一个该交付的分片序号 `nextOut`；worker 完成的结果先进入
  `reorder` 缓冲，只有当 `nextOut` 到达时才写出。所以即使高并发，`results` 中的
  输出顺序严格等于 `Shard.Index` 升序，即串行读取顺序。
- 对**顺序型来源**，C_s = 1 且“单源槽”在 collector **按序回收该片结果后**才释放。
  因此下一张顺序分片只会在上一张结果被处理完后派发，ReadShard 收到的 start 始终
  等于当前游标，不会出现回退或重叠。

## 3. 背压与内存上界

在途数据指“已派发（已分配 `[]byte`）但尚未按序交付给消费端”的所有分片数据。
collector 在把分片写入慢目标的**成功之后**才通过 reclaim 通知 dispatcher 释放预算。
因此即使消费端很慢：

- 在途数据总字节数始终 ≤ `InFlightBytes`；
- 加上每源在途分片数 ≤ C_s、全局 ≤ W，内存上界可估算为：
  `min(B, W * maxShardSize, Σ C_s * maxShardSize)` 量级的缓冲加上通道开销。

请按数据大小与可接受延迟合理设置：B 越小背压越早（内存更省、并发度可能降低），
越大吞吐越高但占用更多内存。默认 `GlobalWorkers` 规范化为 ≥1，`InFlightBytes`
默认 64 MiB。

## 4. 取消与超时

- 内部用 `context.WithCancel` 派生 ctx；来源的 `ReadShard` 在循环中检查 ctx，
  1 MiB 级（32 KiB 块）响应取消。
- 任意 worker 失败、任意组件失败，或上游 ctx 取消/超时，都调用内部 cancel，
  取消**全部**在途读取，其余来源的在途分片同步终止。
- 生命周期以 collector 结束为准；Run 返回前会：cancel → 等 dispatcher 退出 →
  关闭 workers → `wg.Wait()` 等所有读取 goroutine 退出。故 Run 返回后**没有**
  未完成的处理、没有遗留 goroutine。
- 错误归因优先取实际读取错误（如来源变化/权限），只有上游 ctx 确实被取消或
  超时才归类为 `KindCanceled` / `KindTimeout`——内部用于收尾的 cancel 不会被误报。

## 5. 为什么令牌不由 worker 自己归还

若 worker 读完立即归还单源令牌，dispatcher 可能在上一张顺序片结果**尚未被 collector
回收**前就派发下一张，对顺序设备形成重叠请求（游标被越过），并且与无缓冲 worker
通道组合时容易形成 dispatcher↔worker 的循环等待。本实现让“单源空额 + 字节预算”
统一由 collector 按序回收后释放，既保证单调又避免循环等待。
## 6. 重启后的单调恢复

对顺序源，任务恢复时计算该来源已确认的**连续前缀**长度 L（按其分片序列，遇到第一个
未确认分片即停止），调用 `ResumeAt(L)` 把游标推进过该前缀（内部 seek 过该区间，不
重新处理字节），然后再进入流水线。随机源没有游标的概念，已确认分片直接跳过。
