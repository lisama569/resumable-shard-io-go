package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

// Logger is the minimal logging surface used by the pipeline. Every
// decision (skip/reprocess/abort) is logged with the shard range, the
// checkpoint version and the basis for the decision.
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Options configures a Run.
type Options struct {
	// Plan is the validated source/shard description.
	Plan Plan
	// OutputPath is the file the concatenated stream is merged into.
	// It must not coincide with any source path.
	OutputPath string
	// Checkpoints persists progress; nil disables resume (every run starts
	// from scratch and nothing is confirmed durably).
	Checkpoints CheckpointStore
	// Concurrency is the global ceiling of shards in flight. Per-source
	// MaxConcurrency is an additional ceiling. <=0 means GOMAXPROCS.
	Concurrency int
	// Logger receives structured decision logs; nil discards them.
	Logger Logger
}

// Report summarizes one Run invocation.
type Report struct {
	Shards       int
	Reused       int // shards proven complete from checkpoint + output readback
	Processed    int // shards actually read from sources during this call
	Skipped      int // duplicate completion acknowledged as no-ops
	TotalBytes   int64
	MergedSHA256 string // SHA-256 of the final concatenated output
}

// Run executes the sharded read/verify/merge pipeline with resumability.
type Run struct {
	opts Options
	log  Logger
}

// NewRun constructs a Run.
func NewRun(opts Options) *Run {
	log := opts.Logger
	if log == nil {
		log = discardLogger{}
	}
	return &Run{opts: opts, log: log}
}

// progress is the serialized checkpoint state. All mutation and every
// durable Save happen under mu, so duplicate/out-of-order completion is
// idempotent and saves never tear each other.
type progress struct {
	mu sync.Mutex
	cp *Checkpoint
}

func (p *progress) confirm(ctx context.Context, store CheckpointStore, idx int, sum []byte) (duplicate bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hexsum := hex.EncodeToString(sum)
	if existing, ok := p.cp.Completed[idx]; ok {
		// Idempotent duplicate submission: same verified generation stays;
		// a conflicting digest is a hard error, never a silent overwrite.
		if existing == hexsum {
			return true, nil
		}
		return false, fmt.Errorf("%w: shard %d already confirmed with checksum %s, refusing %s",
			ErrShardRejected, idx, existing, hexsum)
	}
	p.cp.Completed[idx] = hexsum
	if len(p.cp.Completed) == p.cp.ShardCount {
		p.cp.Finished = true
	}
	if store != nil {
		if err := store.Save(ctx, p.cp); err != nil {
			delete(p.cp.Completed, idx)
			p.cp.Finished = false
			return false, err
		}
	}
	return false, nil
}

// Run executes to completion or returns the first classified error.
//
// Failure isolation is fail-fast per task and fail-stop per run: a shard
// error cancels every other in-flight and pending shard, already confirmed
// shards remain durable and resumable, and unconfirmed bytes are never
// merged-as-success. No retry across generations happens inside one run;
// re-invoking Run after fixing the underlying cause resumes deterministically.
func (r *Run) Run(ctx context.Context) (*Report, error) {
	if err := ctx.Err(); err != nil {
		return nil, r.ctxError(ctx)
	}
	conc := r.opts.Concurrency
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
		if conc < 1 {
			conc = 1
		}
	}

	// Phase 1: bind the run to one source generation and partition it.
	bp, err := buildPlan(ctx, r.opts.Plan)
	if err != nil {
		return nil, err
	}
	srcByName := make(map[string]Source, len(r.opts.Plan.Sources))
	for _, s := range r.opts.Plan.Sources {
		srcByName[s.Name()] = s
		if fp := bp.fingerprints[s.Name()]; fp.Source == r.opts.OutputPath {
			return nil, fmt.Errorf("%w: output path %q is also source %q",
				ErrInvalidPlan, r.opts.OutputPath, s.Name())
		}
	}
	r.log.Printf("plan: version=%d shards=%d total=%d shardSize=%d fingerprints={%s}",
		checkpointVersion, len(bp.shards), bp.totalSize, r.opts.Plan.ShardSize,
		canonicalFingerprintText(bp.fingerprints))

	// Phase 2: open the merge target before any checkpoint decision.
	out, err := openOutput(r.opts.OutputPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	// Phase 3: load or initialize the checkpoint, then prove the bound
	// generation matches and which output regions are already durable.
	prog := &progress{}
	var stored *Checkpoint
	if r.opts.Checkpoints != nil {
		stored, err = r.opts.Checkpoints.Load(ctx)
		if err != nil {
			return nil, err
		}
	}
	switch {
	case stored == nil:
		if fi, statErr := out.Stat(); statErr == nil && fi.Size() != 0 {
			r.log.Printf("fresh run: truncating unclaimed output %q (size=%d, no checkpoint)",
				r.opts.OutputPath, fi.Size())
		}
		if err := out.Truncate(0); err != nil {
			return nil, fmt.Errorf("prepare output: %w", classifyOSError(err))
		}
		prog.cp = newCheckpoint(bp, r.opts)
	default:
		if err := verifyForResume(stored, bp, r.opts.Plan.ShardSize, r.opts.OutputPath); err != nil {
			return nil, err
		}
		prog.cp = stored
	}

	completed := make(map[int]string, len(prog.cp.Completed))
	for k, v := range prog.cp.Completed {
		completed[k] = v
	}

	// Verify claimed regions against the actual output file. A mismatch
	// (torn write, truncated file, externally edited region) is repaired by
	// reprocessing that shard; the basis is logged explicitly.
	var pending []Shard
	reused := 0
	for _, sh := range bp.shards {
		want, ok := completed[sh.Index]
		if !ok {
			pending = append(pending, sh)
			continue
		}
		got, err := hashOutputRegion(ctx, out, sh)
		if err == nil && got == want {
			reused++
			r.log.Printf("shard %d source=%s range=[%d,%d): reuse (basis: checkpoint version=%d checksum %s matches output readback)",
				sh.Index, sh.SourceName, sh.Offset, sh.Offset+sh.Length, prog.cp.Version, want)
			continue
		}
		r.log.Printf("shard %d source=%s range=[%d,%d): reprocess (basis: output readback %q != checkpoint %q: %v)",
			sh.Index, sh.SourceName, sh.Offset, sh.Offset+sh.Length, got, want, err)
		pending = append(pending, sh)
	}
	r.log.Printf("resume: checkpoint version=%d plan=%s completed=%d/%d pending=%d decision=%s",
		prog.cp.Version, prog.cp.PlanID[:12], len(completed), len(bp.shards), len(pending),
		map[bool]string{true: "resume", false: "fresh"}[stored != nil])

	if prog.cp.Finished && len(pending) == 0 {
		// Duplicate submission of an already finished, verified run.
		sum, err := hashWholeOutput(ctx, out, bp.totalSize)
		if err != nil {
			return nil, err
		}
		r.log.Printf("run already finished: verified %d shards, merged checksum=%s; no sources read",
			len(bp.shards), sum)
		return &Report{Shards: len(bp.shards), Reused: reused, Skipped: len(bp.shards),
			TotalBytes: bp.totalSize, MergedSHA256: sum}, nil
	}

	// Phase 4: bounded concurrent processing.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(e error) {
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	globalSem := make(chan struct{}, conc)
	sourceSems := make(map[string]chan struct{}, len(srcByName))
	for name, s := range srcByName {
		sourceSems[name] = make(chan struct{}, s.MaxConcurrency())
	}
	acquire := func(sem chan struct{}) error {
		select {
		case sem <- struct{}{}:
			return nil
		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
	release := func(sem chan struct{}) { <-sem }

	process := func(sh Shard) {
		r.log.Printf("shard %d source=%s range=[%d,%d) srcOffset=%d len=%d: start",
			sh.Index, sh.SourceName, sh.Offset, sh.Offset+sh.Length, sh.SrcOffset, sh.Length)
		sum, err := r.processShard(runCtx, srcByName, bp.fingerprints, out, sh)
		if err != nil {
			setErr(err)
			return
		}
		dup, err := prog.confirm(runCtx, r.opts.Checkpoints, sh.Index, sum)
		if err != nil {
			setErr(err)
			return
		}
		r.log.Printf("shard %d source=%s range=[%d,%d): confirmed checksum=%s bytes=%d duplicate=%t (basis: source-read hash equals post-write output readback; fsynced before checkpoint save)",
			sh.Index, sh.SourceName, sh.Offset, sh.Offset+sh.Length, hex.EncodeToString(sum), sh.Length, dup)
	}

	// Random-access shards: shared task channel, global worker pool, and an
	// additional per-source semaphore enforcing each source's own ceiling.
	var raTasks []Shard
	var seqGroups [][]Shard
	seqOrder := make([]string, 0)
	for _, sh := range pending {
		if srcByName[sh.SourceName].Kind() == KindRandomAccess {
			raTasks = append(raTasks, sh)
		}
	}
	seqByName := map[string][]Shard{}
	for _, sh := range pending {
		if srcByName[sh.SourceName].Kind() == KindSequential {
			if _, seen := seqByName[sh.SourceName]; !seen {
				seqOrder = append(seqOrder, sh.SourceName)
			}
			seqByName[sh.SourceName] = append(seqByName[sh.SourceName], sh)
		}
	}
	for _, name := range seqOrder {
		seqGroups = append(seqGroups, seqByName[name])
	}

	taskCh := make(chan Shard, len(raTasks))
	for _, sh := range raTasks {
		taskCh <- sh
	}
	close(taskCh)

	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Global ceiling first; releasing a permit makes exit
				// observable immediately under cancellation.
				if err := acquire(globalSem); err != nil {
					return
				}
				select {
				case sh, ok := <-taskCh:
					if !ok {
						release(globalSem)
						return
					}
					sem := sourceSems[sh.SourceName]
					if err := acquire(sem); err != nil {
						release(globalSem)
						return
					}
					process(sh)
					release(sem)
					release(globalSem)
				case <-runCtx.Done():
					release(globalSem)
					return
				}
			}
		}()
	}

	// Sequential sources: one driver each, strictly in shard order, one
	// in-flight stream per source, sharing the global ceiling.
	for _, group := range seqGroups {
		wg.Add(1)
		go func(shards []Shard) {
			defer wg.Done()
			sem := sourceSems[shards[0].SourceName]
			for _, sh := range shards {
				if err := acquire(globalSem); err != nil {
					return
				}
				if err := acquire(sem); err != nil {
					release(globalSem)
					return
				}
				process(sh)
				release(sem)
				release(globalSem)
			}
		}(group)
	}
	wg.Wait()

	// If the parent context ended while the last successful workers were
	// draining, pending shards may simply have been dropped rather than
	// failed: that is a canceled run, never a partial success.
	if firstErr == nil && ctx.Err() != nil {
		firstErr = fmt.Errorf("%w: %v", ErrCanceled, ctx.Err())
	}
	if firstErr != nil {
		r.log.Printf("run aborted: %v; confirmed=%d/%d remain durable and resumable",
			firstErr, len(prog.cp.Completed), len(bp.shards))
		return nil, r.normalizeError(ctx, firstErr)
	}

	// Phase 5: ordered final verification of the merged file. Completion
	// order is irrelevant: every region is checked at its fixed offset, so
	// the result equals a strictly serial read of the sources.
	if fi, err := out.Stat(); err != nil {
		return nil, classifyOSError(err)
	} else if fi.Size() != bp.totalSize {
		return nil, fmt.Errorf("%w: output size %d != planned %d", ErrShardRejected, fi.Size(), bp.totalSize)
	}
	merged, err := hashWholeOutput(ctx, out, bp.totalSize)
	if err != nil {
		return nil, err
	}
	verified := 0
	for _, sh := range bp.shards {
		got, err := hashOutputRegion(ctx, out, sh)
		if err != nil {
			return nil, err
		}
		if got != prog.cp.Completed[sh.Index] {
			return nil, fmt.Errorf("%w: final verification of shard %d range=[%d,%d): %s != %s",
				ErrShardRejected, sh.Index, sh.Offset, sh.Offset+sh.Length, got, prog.cp.Completed[sh.Index])
		}
		verified++
	}
	r.log.Printf("run complete: shards=%d verified=%d total=%d merged_checksum=%s",
		verified, verified, bp.totalSize, merged)
	return &Report{
		Shards:       len(bp.shards),
		Reused:       reused,
		Processed:    len(pending),
		TotalBytes:   bp.totalSize,
		MergedSHA256: merged,
	}, nil
}

// processShard reads one shard from its bound generation, streams it into
// the output at the shard's fixed offset, fsyncs, and proves the write by
// reading the region back. Fingerprints are taken immediately before and
// after the read: any change aborts the shard with ErrSourceChanged.
//
// Memory per in-flight shard is one readChunk buffer regardless of shard
// size; a slow destination blocks the worker while holding its permits,
// which is the pipeline's backpressure (hard bound: Concurrency*readChunk
// bytes of staging memory).
func (r *Run) processShard(ctx context.Context, srcs map[string]Source, bound map[string]Fingerprint, out *os.File, sh Shard) ([]byte, error) {
	src := srcs[sh.SourceName]
	if err := checkGeneration(ctx, src, bound[sh.SourceName]); err != nil {
		return nil, err
	}

	h := sha256.New()
	writeCursor := sh.Offset
	handleChunk := func(p []byte) error {
		h.Write(p)
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := out.WriteAt(p, writeCursor)
		if err != nil {
			return fmt.Errorf("write shard %d at offset %d: %w", sh.Index, writeCursor, classifyOSError(err))
		}
		if n != len(p) {
			return fmt.Errorf("%w: short write for shard %d (%d/%d)", ErrShardRejected, sh.Index, n, len(p))
		}
		writeCursor += int64(n)
		return nil
	}

	var readN int64
	// A bounded writer into the output file at a fixed offset: no
	// intermediate shard-sized buffer exists, so a slow destination
	// applies backpressure directly to the source read.
	emit := writerFunc(func(p []byte) error {
		if err := handleChunk(p); err != nil {
			return err
		}
		readN += int64(len(p))
		return nil
	})
	// Dispatch on the declared capability, not on the method set: one
	// concrete type may implement both interfaces.
	switch src.Kind() {
	case KindRandomAccess:
		s := src.(RandomAccessSource)
		if _, err := s.ReadRange(ctx, sh.SrcOffset, sh.Length, emit); err != nil {
			return nil, err
		}
	case KindSequential:
		s := src.(SequentialSource)
		rc, err := s.ReadStream(ctx, sh.SrcOffset, sh.Length)
		if err != nil {
			return nil, err
		}
		if _, cerr := io.Copy(emit, rc); cerr != nil {
			_ = rc.Close()
			return nil, cerr
		}
		if err := rc.Close(); err != nil {
			return nil, classifyOSError(err)
		}
	default:
		return nil, fmt.Errorf("%w: source %q has neither access capability", ErrInvalidPlan, sh.SourceName)
	}
	if readN != sh.Length {
		return nil, fmt.Errorf("%w: shard %d source=%s delivered %d bytes, want %d",
			ErrShardRejected, sh.Index, sh.SourceName, readN, sh.Length)
	}

	// Post-read generation check: bytes must come from one generation only.
	if err := checkGeneration(ctx, src, bound[sh.SourceName]); err != nil {
		return nil, err
	}

	// Durability before acknowledgment.
	if err := out.Sync(); err != nil {
		return nil, fmt.Errorf("fsync output after shard %d: %w", sh.Index, classifyOSError(err))
	}

	// Readback proof: the merged region is exactly what was read.
	readback, err := hashOutputRegion(ctx, out, sh)
	if err != nil {
		return nil, err
	}
	want := hex.EncodeToString(h.Sum(nil))
	if readback != want {
		return nil, fmt.Errorf("%w: shard %d readback %s != read hash %s",
			ErrShardRejected, sh.Index, readback, want)
	}
	return h.Sum(nil), nil
}

type writerFunc func([]byte) error

func (f writerFunc) Write(p []byte) (int, error) {
	if err := f(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// checkGeneration re-fingerprints a source and compares it with the
// generation the run planned against.
func checkGeneration(ctx context.Context, src Source, want Fingerprint) error {
	got, err := src.Fingerprint(ctx)
	if err != nil {
		return fmt.Errorf("source %q generation check: %w", src.Name(), classifyOSError(err))
	}
	if got.Name == "" {
		got.Name = want.Name
	}
	if got != want {
		return fmt.Errorf("%w: %q bound to {size=%d stamp=%d backend=%q} but observed {size=%d stamp=%d backend=%q}",
			ErrSourceChanged, src.Name(), want.Size, want.Stamp, want.Source, got.Size, got.Stamp, got.Source)
	}
	return nil
}

func newCheckpoint(bp builtPlan, opts Options) *Checkpoint {
	return &Checkpoint{
		Version:      checkpointVersion,
		PlanID:       bp.planID,
		OutputPath:   opts.OutputPath,
		ShardSize:    opts.Plan.ShardSize,
		TotalSize:    bp.totalSize,
		ShardCount:   len(bp.shards),
		Fingerprints: bp.fingerprints,
		Completed:    map[int]string{},
	}
}

func openOutput(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open output %q: %w", path, classifyOSError(err))
	}
	return f, nil
}

// hashOutputRegion reads [sh.Offset, sh.Offset+sh.Length) from the merged
// file and returns its hex SHA-256.
func hashOutputRegion(ctx context.Context, f *os.File, sh Shard) (string, error) {
	h := sha256.New()
	buf := make([]byte, readChunk)
	var pos int64
	for pos < sh.Length {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n := int64(len(buf))
		if sh.Length-pos < n {
			n = sh.Length - pos
		}
		rn, err := f.ReadAt(buf[:n], sh.Offset+pos)
		if rn > 0 {
			h.Write(buf[:rn])
			pos += int64(rn)
		}
		if err != nil {
			if err == io.EOF && pos == sh.Length {
				break
			}
			return "", fmt.Errorf("%w: readback shard %d at %d: %v", ErrShardRejected, sh.Index, sh.Offset+pos, err)
		}
	}
	if pos != sh.Length {
		return "", fmt.Errorf("%w: readback shard %d got %d/%d bytes", ErrShardRejected, sh.Index, pos, sh.Length)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashWholeOutput(ctx context.Context, f *os.File, size int64) (string, error) {
	h := sha256.New()
	buf := make([]byte, readChunk)
	var pos int64
	for pos < size {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n := int64(len(buf))
		if size-pos < n {
			n = size - pos
		}
		rn, err := f.ReadAt(buf[:n], pos)
		if rn > 0 {
			h.Write(buf[:rn])
			pos += int64(rn)
		}
		if err != nil && !(err == io.EOF && pos == size) {
			return "", fmt.Errorf("final readback at %d: %w", pos, classifyOSError(err))
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (r *Run) ctxError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: deadline exceeded before start", ErrCanceled)
	}
	return fmt.Errorf("%w: %v", ErrCanceled, ctx.Err())
}

// normalizeError preserves internal classified errors; a plain context
// completion (cancel/timeout) is reported as ErrCanceled.
func (r *Run) normalizeError(origCtx context.Context, err error) error {
	if errors.Is(err, ErrCheckpointCorrupt) ||
		errors.Is(err, ErrCheckpointVersion) ||
		errors.Is(err, ErrCheckpointMismatch) ||
		errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrSourceChanged) ||
		errors.Is(err, ErrShardRejected) ||
		errors.Is(err, ErrInvalidPlan) {
		return err
	}
	if origCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrCanceled, err)
	}
	return err
}
