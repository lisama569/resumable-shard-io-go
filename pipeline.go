package shardio

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// PipelineConfig 控制并发与内存上界。
type PipelineConfig struct {
	// GlobalWorkers 是全局同时进行的读取操作数上限（>=1）。
	GlobalWorkers int
	// InFlightBytes 是“已派发但尚未按序交付”的分片数据字节上界。
	// 达到上界时派发暂停（背压），从而为慢目标/高并发提供明确内存上界。
	InFlightBytes int64
	// MaxShardBytes 是单个分片允许的最大字节数（0 表示不额外限制）。
	MaxShardBytes int64
	// Logger 用于打印分片范围与判定依据；nil 时使用 slog.Default()。
	Logger *slog.Logger
}

// job 是调度器发给 worker 的内部任务。
type job struct {
	shard Shard
}

// workerResult 是 worker 回报给 collector 的结果（可能带错）。
type workerResult struct {
	idx    int
	source string
	res    ShardResult
	err    error
	bytes  int64
}

// Pipeline 以受限并发执行分片读取，按分片序号有序投递结果。
type Pipeline struct {
	cfg PipelineConfig
}

// NewPipeline 创建流水线并规范化配置。
func NewPipeline(cfg PipelineConfig) *Pipeline {
	if cfg.GlobalWorkers < 1 {
		cfg.GlobalWorkers = 1
	}
	if cfg.InFlightBytes <= 0 {
		cfg.InFlightBytes = 64 << 20 // 默认 64MiB 在途上界
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Pipeline{cfg: cfg}
}

// Run 执行分片计划。
//
//   - sources 提供读取能力；done 中已存在的分片视为已确认落盘，不读取、不投递；
//   - 结果严格按 Shard.Index 升序写入 results；
//   - 顺序型来源严格按偏移单调读取，单源并发恒为 1，且下一张分片只在上一张
//     结果被 collector 回收后才派发；
//   - 随机访问型来源受各自 MaxConcurrency 上限约束；全局并发受 GlobalWorkers 约束；
//   - 在途字节受 InFlightBytes 约束（慢目标下内存仍有界）；
//   - 任何来源失败或 ctx 取消/超时都会取消全部在途读取，不投递乱序结果；
//   - Run 返回后所有内部 goroutine 均已退出（无未完成处理），results 被关闭。
//
// 调用方必须持续消费 results 直到 Run 返回。
func (p *Pipeline) Run(
	ctx context.Context,
	sources map[string]Source,
	shards []Shard,
	done map[int]bool,
	results chan<- ShardResult,
) (retErr error) {
	log := p.cfg.Logger
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer close(results)

	pending := make(map[int]workerResult)
	var inFlightBytes int64

	// 每个分片读取完成都会回收：(字节数, 来源)。collector 在“按序回收该分片”
	// 的时刻通知 dispatcher，既释放在途预算，也释放该来源的下一张派发许可。
	reclaim := make(chan workerResult, len(shards))
	collected := make(chan workerResult, len(shards))
	dispatchErr := make(chan error, 1)

	// 单源并发计数器（在 dispatcher 单线程内维护，无需锁）：
	//   active[sid] = 已派发但尚未被 collector 按序回收的分片数；
	// 随机源上限为 MaxConcurrency；顺序源上限恒为 1（=> 严格单调 + 串行）。
	active := make(map[string]int, len(sources))
	capOf := func(sid string) int {
		if s := sources[sid]; s != nil && s.Kind() == Sequential {
			return 1
		}
		if s := sources[sid]; s != nil {
			c := s.MaxConcurrency()
			if c < 1 {
				c = 1
			}
			return c
		}
		return 1
	}

	// workers 为带缓冲通道，容量 = GlobalWorkers：天然限定最多 N 个在途读取，
	// 同时 dispatcher 绝不会因“发送 job”与 worker“归还令牌”互相等待。
	workers := make(chan job, p.cfg.GlobalWorkers)
	var wg sync.WaitGroup
	wg.Add(p.cfg.GlobalWorkers)
	for w := 0; w < p.cfg.GlobalWorkers; w++ {
		go func() {
			defer wg.Done()
			for j := range workers {
				data, err := sources[j.shard.SourceID].ReadShard(ctx, j.shard.Start, j.shard.End)
				wr := workerResult{idx: j.shard.Index, source: j.shard.SourceID, bytes: j.shard.Length()}
				if err != nil {
					wr.err = err
				} else {
					wr.res = ShardResult{Shard: j.shard, Data: data, Checksum: checksumBytes(data)}
				}
				select {
				case collected <- wr:
				case <-ctx.Done():
				}
			}
		}()
	}

	// ---- dispatcher：唯一操作 active/inFlightBytes 的 goroutine ----
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		fail := func(err error) {
			select {
			case dispatchErr <- err:
			default:
			}
			cancel()
		}
		for _, sh := range shards {
			if done[sh.Index] {
				log.DebugContext(ctx, "shard skipped (already confirmed)",
					slog.Int("index", sh.Index), slog.String("source", sh.SourceID),
					slog.Int64("start", sh.Start), slog.Int64("end", sh.End))
				continue
			}
			if _, ok := sources[sh.SourceID]; !ok {
				fail(newError(KindInvalidArgument, sh.SourceID,
					fmt.Sprintf("no source registered for shard %d", sh.Index), nil))
				return
			}
			size := sh.Length()
			if p.cfg.MaxShardBytes > 0 && size > p.cfg.MaxShardBytes {
				fail(newError(KindInvalidArgument, sh.SourceID,
					fmt.Sprintf("shard %d size %d exceeds MaxShardBytes %d", sh.Index, size, p.cfg.MaxShardBytes), nil))
				return
			}

			// 等待直到：单源并发有空额、在途字节有预算。
			// collector 每按序回收一张分片，就通过 reclaim 通知一次。
			for active[sh.SourceID] >= capOf(sh.SourceID) || inFlightBytes+size > p.cfg.InFlightBytes {
				if active[sh.SourceID] >= capOf(sh.SourceID) {
					log.DebugContext(ctx, "waiting for source slot",
						slog.String("source", sh.SourceID),
						slog.Int("active", active[sh.SourceID]),
						slog.Int("cap", capOf(sh.SourceID)))
				} else {
					log.DebugContext(ctx, "backpressure: waiting for in-flight budget",
						slog.Int64("in_flight_bytes", inFlightBytes),
						slog.Int64("limit", p.cfg.InFlightBytes), slog.Int64("need", size))
				}
				select {
				case rc := <-reclaim:
					active[rc.source]--
					inFlightBytes -= rc.bytes
					if active[rc.source] < 0 {
						active[rc.source] = 0
					}
					if inFlightBytes < 0 {
						inFlightBytes = 0
					}
				case <-ctx.Done():
					return
				}
			}

			active[sh.SourceID]++
			inFlightBytes += size
			log.DebugContext(ctx, "shard dispatched",
				slog.Int("index", sh.Index), slog.String("source", sh.SourceID),
				slog.String("kind", string(sources[sh.SourceID].Kind())),
				slog.Int64("start", sh.Start), slog.Int64("end", sh.End),
				slog.Int64("bytes", size), slog.Int64("in_flight_bytes", inFlightBytes))

			select {
			case workers <- job{shard: sh}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// ---- collector：按序交付；按序回收时通知 dispatcher 释放预算/空额 ----
	collectorDone := make(chan struct{})
	var collectErr error
	go func() {
		defer close(collectorDone)
		total := 0
		for _, sh := range shards {
			if !done[sh.Index] {
				total++
			}
		}
		nextOut := 0
		received := 0
		for received < total {
			for nextOut < len(shards) && done[nextOut] {
				nextOut++
			}
			var wr workerResult
			select {
			case wr = <-collected:
			case <-ctx.Done():
				collectErr = classifyCtxErr(ctx, ctx.Err())
				return
			}
			received++
			if _, dup := pending[wr.idx]; dup {
				collectErr = newError(KindInvalidArgument, "",
					fmt.Sprintf("internal: duplicate result for shard %d", wr.idx), nil)
				cancel()
				return
			}
			pending[wr.idx] = wr

			for {
				for nextOut < len(shards) && done[nextOut] {
					nextOut++
				}
				if nextOut >= len(shards) {
					return
				}
				wr, ok := pending[nextOut]
				if !ok {
					break
				}
				delete(pending, nextOut)
				if wr.err != nil {
					collectErr = wr.err
					cancel()
					return
				}
				// 按序交付给（可能很慢的）目标；交付成功才回收预算/空额，
				// 从而慢目标下在途数据与每源在途分片都被封住。
				select {
				case results <- wr.res:
					log.DebugContext(ctx, "shard delivered in order",
						slog.Int("index", wr.res.Shard.Index),
						slog.String("source", wr.res.Shard.SourceID),
						slog.Int64("start", wr.res.Shard.Start), slog.Int64("end", wr.res.Shard.End),
						slog.String("checksum", wr.res.Checksum))
				case <-ctx.Done():
					collectErr = classifyCtxErr(ctx, ctx.Err())
					return
				}
				reclaim <- wr
				nextOut++
			}
		}
	}()

	// ---- 生命周期：成功或失败都以 collector 结束为准 ----
	select {
	case <-collectorDone:
	case <-ctx.Done():
		<-collectorDone
	}
	cancel()
	<-dispatchDone
	close(workers)
	wg.Wait() // 全部在途读取 goroutine 退出，不留未完成处理

	switch {
	case collectErr != nil:
		retErr = collectErr
	default:
		select {
		case e := <-dispatchErr:
			retErr = e
		default:
			retErr = classifyCtxErr(parentCtx, parentCtx.Err())
		}
	}
	log.DebugContext(ctx, "pipeline finished",
		slog.Int("shards_total", len(shards)),
		slog.Int("shards_confirmed", len(done)),
		slog.Any("error", retErr))
	return retErr
}
