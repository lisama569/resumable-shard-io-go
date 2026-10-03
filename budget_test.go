package shardio

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// 场景 8：在途字节预算 < 单个分片 —— 必须立即以 KindBudgetTooSmall 失败，
// 不能无限等待；错误信息要带预算值与所需值，便于运维调整。
func TestBudget_InFlightBelowOneShard_FailsFast(t *testing.T) {
	data := bytes.Repeat([]byte{'Z'}, 500)
	s := &memSource{id: "s", kind: RandomAccess, data: data, conc: 4}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100) // 每片 100B
	results := make(chan ShardResult)
	done := make(chan error, 1)
	go func() {
		done <- NewPipeline(PipelineConfig{GlobalWorkers: 1, InFlightBytes: 99}).
			Run(context.Background(), map[string]Source{"s": s}, shards, nil, results)
	}()
	// 失败路径也必须关闭 results（无 goroutine 泄漏）：排空直到通道关闭。
	closed := make(chan struct{})
	go func() {
		for range results {
		}
		close(closed)
	}()
	select {
	case err := <-done:
		var e *Error
		if !errors.As(err, &e) || e.Kind != KindBudgetTooSmall {
			t.Fatalf("want KindBudgetTooSmall, got %#v", err)
		}
		if !errors.Is(err, ErrBudgetTooSmall) {
			t.Fatalf("errors.Is(ErrBudgetTooSmall) failed: %#v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline hung instead of failing fast on a sub-shard budget")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("results channel not closed after budget failure")
	}
	if n := s.readsStarted.Load(); n != 0 {
		t.Fatalf("no shard may be read before budget rejection, got %d reads", n)
	}
}

// 场景 9：预算恰好等于单个分片大小（且 GlobalWorkers=1）——任务在有限时间内
// 正常串行完成，结果与高预算完全一致；运行期间在途分片不超过 1。
func TestBudget_ExactlyOneShard_CompletesSequentially(t *testing.T) {
	data := bytes.Repeat([]byte{'Q'}, 1000)
	s := &memSource{id: "s", kind: RandomAccess, data: data, conc: 4}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	results := make(chan ShardResult)
	done := make(chan error, 1)
	go func() {
		done <- NewPipeline(PipelineConfig{GlobalWorkers: 1, InFlightBytes: 100}).
			Run(context.Background(), map[string]Source{"s": s}, shards, nil, results)
	}()
	var merged bytes.Buffer
	n := 0
	for r := range results {
		merged.Write(r.Data)
		n++
		// 在途并发上界：预算只够 1 片，任何时刻开始读取的片完成后才会派发下一片。
		// memSource 的 active 在读取持有期间计数；此处消费端串行，峰值恒 ≤1。
		if v := s.active.Load(); v > 1 {
			t.Fatalf("in-flight shards %d exceeded budget-forced cap 1", v)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("tight-but-valid budget must complete: %v", err)
	}
	if n != len(shards) {
		t.Fatalf("want %d shards, got %d", len(shards), n)
	}
	if !bytes.Equal(merged.Bytes(), data) {
		t.Fatal("tight-budget output differs from full-budget serial result")
	}
	if s.maxSeen.Load() > 1 {
		t.Fatalf("peak source concurrency %d > 1 with single-shard budget", s.maxSeen.Load())
	}
}

// 场景 10：极小预算不影响“已完成分片跳过”——全部完成的恢复任务即使预算为 1
// 也应成功空跑（预算只约束待处理分片）。
func TestBudget_TinyBudgetAllCompleted_IsIdempotent(t *testing.T) {
	data := bytes.Repeat([]byte{'K'}, 500)
	s := &memSource{id: "s", kind: Sequential, data: data, failAfter: 1}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	done := map[int]bool{}
	for _, sh := range shards {
		done[sh.Index] = true
	}
	_, err := runPipeline(t, PipelineConfig{GlobalWorkers: 1, InFlightBytes: 1},
		map[string]Source{"s": s}, shards, done)
	if err != nil {
		t.Fatalf("all-completed plan with 1-byte budget must succeed: %v", err)
	}
}

// 场景 11：任务级极小预算 —— 未开始的任务在 Run 中即明确失败（可区分于取消/超时）。
func TestBudget_TaskRunWithSubShardBudget_FailsClearly(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := doctorCfg(t, dir, 10, 'A')
	cfg.Pipeline = PipelineConfig{GlobalWorkers: 1, InFlightBytes: 32} // ShardSize=64
	task, err := NewTask(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = task.Run(context.Background())
	if !kindIs(err, KindBudgetTooSmall) {
		t.Fatalf("want KindBudgetTooSmall from Task.Run, got %#v", err)
	}
}
