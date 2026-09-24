package shardio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 测试用内存来源 ----

type memSource struct {
	id           string
	kind         SourceKind
	data         []byte
	conc         int
	active       atomic.Int64
	maxSeen      atomic.Int64
	failAfter    int64 // 0=不失败；>0 时第 failAfter 次调用失败
	calls        atomic.Int64
	readsStarted atomic.Int64
	gate         chan struct{} // 非 nil 时读取前阻塞于此
	cursorMu     sync.Mutex
	cursor       int64
}

func (m *memSource) ID() string          { return m.id }
func (m *memSource) Kind() SourceKind    { return m.kind }
func (m *memSource) MaxConcurrency() int { return m.conc }
func (m *memSource) Size() int64         { return int64(len(m.data)) }
func (m *memSource) Close() error        { return nil }

func (m *memSource) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return Fingerprint{Size: int64(len(m.data)), Hash: fmt.Sprintf("mem-%x", len(m.data)), Mod: "t0"}, nil
}

func (m *memSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	if m.kind == Sequential {
		m.cursorMu.Lock()
		if start != m.cursor {
			m.cursorMu.Unlock()
			return nil, newError(KindInvalidArgument, m.id,
				fmt.Sprintf("non-monotonic sequential read: cursor=%d start=%d", m.cursor, start), nil)
		}
		m.cursor = end
		m.cursorMu.Unlock()
	}
	m.readsStarted.Add(1)
	cur := m.active.Add(1)
	for {
		old := m.maxSeen.Load()
		if cur <= old || m.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}
	defer m.active.Add(-1)
	if m.gate != nil {
		select {
		case <-m.gate:
		case <-ctx.Done():
			return nil, classifyCtxErr(ctx, ctx.Err())
		}
	}
	if m.failAfter > 0 {
		if m.calls.Add(1) >= m.failAfter {
			return nil, newError(KindSourceUnreadable, m.id, "injected read failure", nil)
		}
	}
	if end > int64(len(m.data)) {
		return nil, newError(KindSourceUnreadable, m.id, "read past end", io.ErrUnexpectedEOF)
	}
	out := make([]byte, end-start)
	copy(out, m.data[start:end])
	// 模拟少量工作，放大并发交错。
	select {
	case <-time.After(time.Millisecond):
	case <-ctx.Done():
		return nil, classifyCtxErr(ctx, ctx.Err())
	}
	return out, nil
}

func memState(s *memSource) SourceState {
	return SourceState{ID: s.id, Kind: s.kind, MaxConcurrency: s.conc,
		Fingerprint: Fingerprint{Size: int64(len(s.data)), Hash: fmt.Sprintf("mem-%x", len(s.data)), Mod: "t0"}}
}

func runPipeline(t *testing.T, cfg PipelineConfig, sources map[string]Source, shards []Shard, done map[int]bool) ([]ShardResult, error) {
	t.Helper()
	results := make(chan ShardResult, 64)
	var got []ShardResult
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range results {
			got = append(got, r)
		}
	}()
	err := NewPipeline(cfg).Run(context.Background(), sources, shards, done, results)
	wg.Wait()
	return got, err
}

func TestPipeline_OrderedOutputEqualsSerial(t *testing.T) {
	dataA := bytes.Repeat([]byte("A"), 1000)
	dataB := bytes.Repeat([]byte("B"), 700)
	a := &memSource{id: "a", kind: RandomAccess, data: dataA, conc: 3}
	b := &memSource{id: "b", kind: Sequential, data: dataB, conc: 1}
	states := []SourceState{memState(a), memState(b)}
	shards, err := PlanShards(states, 128)
	if err != nil {
		t.Fatal(err)
	}
	srcs := map[string]Source{"a": a, "b": b}

	got, err := runPipeline(t, PipelineConfig{GlobalWorkers: 4, InFlightBytes: 400}, srcs, shards, nil)
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}
	if len(got) != len(shards) {
		t.Fatalf("got %d results, want %d", len(got), len(shards))
	}
	// 严格升序。
	for i, r := range got {
		if r.Shard.Index != i {
			t.Fatalf("result %d delivered shard index %d, want %d", i, r.Shard.Index, i)
		}
	}
	// 合并结果与顺序拼接一致。
	var merged bytes.Buffer
	for _, r := range got {
		merged.Write(r.Data)
	}
	want := append(append([]byte{}, dataA...), dataB...)
	if !bytes.Equal(merged.Bytes(), want) {
		t.Fatalf("merged output does not equal serial concatenation (len %d vs %d)", merged.Len(), len(want))
	}
	// 每片校验和自洽。
	for _, r := range got {
		if r.Checksum != checksumBytes(r.Data) {
			t.Fatalf("bad checksum for shard %d", r.Shard.Index)
		}
	}
}

func TestPipeline_ConcurrencyCaps(t *testing.T) {
	dataA := bytes.Repeat([]byte{1}, 600)
	a := &memSource{id: "a", kind: RandomAccess, data: dataA, conc: 2, gate: make(chan struct{})}
	states := []SourceState{memState(a)}
	shards, _ := PlanShards(states, 50)
	srcs := map[string]Source{"a": a}
	results := make(chan ShardResult, 64)
	done := make(chan struct{})
	pipeErr := make(chan error, 1)
	go func() {
		pipeErr <- NewPipeline(PipelineConfig{GlobalWorkers: 8, InFlightBytes: 1 << 30}).
			Run(context.Background(), srcs, shards, nil, results)
	}()
	go func() {
		for range results {
		}
		close(done)
	}()
	// 让所有 worker 有机会在 gate 前堆积。
	time.Sleep(30 * time.Millisecond)
	if n := a.maxSeen.Load(); n > int64(a.conc) {
		t.Fatalf("source concurrency %d exceeded cap %d", n, a.conc)
	}
	close(a.gate)
	<-done
	if err := <-pipeErr; err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_BackpressureBoundsInFlight(t *testing.T) {
	data := bytes.Repeat([]byte{7}, 1000)
	s := &memSource{id: "s", kind: RandomAccess, data: data, conc: 4}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	// 每片 100B，在途预算 100B：未按序交付的数据不得超过 1 片。
	const limit = int64(100)
	results := make(chan ShardResult)
	firstTaken := make(chan struct{})
	resume := make(chan struct{})
	go func() {
		<-results
		close(firstTaken)
		<-resume // 慢目标：停止消费，制造积压
		for range results {
		}
	}()
	doneRun := make(chan error, 1)
	go func() {
		doneRun <- NewPipeline(PipelineConfig{GlobalWorkers: 8, InFlightBytes: limit}).
			Run(context.Background(), map[string]Source{"s": s}, shards, nil, results)
	}()
	<-firstTaken
	// 慢窗口内：预算被第 1 片占满，开始读取的分片数必须被封住。
	time.Sleep(40 * time.Millisecond)
	if n := s.readsStarted.Load(); n > 3 {
		t.Fatalf("backpressure violated: %d shards started with %d-byte in-flight budget", n, limit)
	}
	close(resume)
	if err := <-doneRun; err != nil {
		t.Fatal(err)
	}
	if n := s.readsStarted.Load(); n != int64(len(shards)) {
		t.Fatalf("want %d total reads, got %d", len(shards), n)
	}
}

func TestPipeline_SequentialMonotonicDispatch(t *testing.T) {
	// 顺序源内部若收到乱序请求即报错；本用例保证调度器始终单调派发。
	data := bytes.Repeat([]byte("S"), 500)
	s := &memSource{id: "seq", kind: Sequential, data: data, conc: 1}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 37)
	got, err := runPipeline(t, PipelineConfig{GlobalWorkers: 8, InFlightBytes: 1 << 20},
		map[string]Source{"seq": s}, shards, nil)
	if err != nil {
		t.Fatalf("sequential dispatch failed: %v", err)
	}
	if len(got) != len(shards) {
		t.Fatalf("got %d shards want %d", len(got), len(shards))
	}
}

func TestPipeline_SourceFailureCancelsEverything(t *testing.T) {
	data := bytes.Repeat([]byte("X"), 2000)
	s := &memSource{id: "s", kind: RandomAccess, data: data, conc: 4, failAfter: 3,
		gate: make(chan struct{})}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	results := make(chan ShardResult, 64)
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewPipeline(PipelineConfig{GlobalWorkers: 4, InFlightBytes: 1 << 20}).
			Run(context.Background(), map[string]Source{"s": s}, shards, nil, results)
	}()
	go func() {
		for range results {
		}
	}()
	time.Sleep(20 * time.Millisecond)
	close(s.gate)
	err := <-errCh
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindSourceUnreadable {
		t.Fatalf("want KindSourceUnreadable, got %#v", err)
	}
}

func TestPipeline_CancelStopsAllWork(t *testing.T) {
	data := bytes.Repeat([]byte("C"), 5000)
	s := &memSource{id: "s", kind: RandomAccess, data: data, conc: 4, gate: make(chan struct{})}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan ShardResult, 64)
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewPipeline(PipelineConfig{GlobalWorkers: 4, InFlightBytes: 1 << 20}).
			Run(ctx, map[string]Source{"s": s}, shards, nil, results)
	}()
	go func() {
		for range results {
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		var e *Error
		if !errors.As(err, &e) || e.Kind != KindCanceled {
			t.Fatalf("want KindCanceled, got %#v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not stop after cancellation")
	}
	// 取消后不得遗留活跃读取。
	time.Sleep(10 * time.Millisecond)
	if n := s.active.Load(); n != 0 {
		t.Fatalf("active reads remain after cancel: %d", n)
	}
}

func TestPipeline_TimeoutClassified(t *testing.T) {
	data := bytes.Repeat([]byte("T"), 500)
	s := &memSource{id: "s", kind: RandomAccess, data: data, gate: make(chan struct{})}
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	results := make(chan ShardResult, 16)
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewPipeline(PipelineConfig{GlobalWorkers: 2, InFlightBytes: 1 << 20}).
			Run(ctx, map[string]Source{"s": s}, shards, nil, results)
	}()
	go func() {
		for range results {
		}
	}()
	err := <-errCh
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindTimeout {
		t.Fatalf("want KindTimeout, got %#v", err)
	}
}

func TestPipeline_SkipCompletedShards(t *testing.T) {
	data := bytes.Repeat([]byte("K"), 500)
	s := &memSource{id: "s", kind: Sequential, data: data, failAfter: 1} // 任何读取都会失败
	states := []SourceState{memState(s)}
	shards, _ := PlanShards(states, 100)
	done := map[int]bool{}
	for _, sh := range shards {
		done[sh.Index] = true
	}
	got, err := runPipeline(t, PipelineConfig{GlobalWorkers: 2, InFlightBytes: 1 << 20},
		map[string]Source{"s": s}, shards, done)
	if err != nil {
		t.Fatalf("all-completed plan should succeed without reads: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 delivered results, got %d", len(got))
	}
}

func TestPipeline_MixedSourcesInOneRun(t *testing.T) {
	ra := &memSource{id: "ra", kind: RandomAccess, data: bytes.Repeat([]byte("R"), 400), conc: 3}
	sq := &memSource{id: "sq", kind: Sequential, data: bytes.Repeat([]byte("Q"), 400), conc: 1}
	states := []SourceState{memState(ra), memState(sq)}
	shards, _ := PlanShards(states, 64)
	got, err := runPipeline(t, PipelineConfig{GlobalWorkers: 5, InFlightBytes: 300},
		map[string]Source{"ra": ra, "sq": sq}, shards, nil)
	if err != nil {
		t.Fatal(err)
	}
	var merged bytes.Buffer
	for _, r := range got {
		merged.Write(r.Data)
	}
	want := append(append([]byte{}, ra.data...), sq.data...)
	if !bytes.Equal(merged.Bytes(), want) {
		t.Fatal("mixed-source merge mismatch")
	}
}
