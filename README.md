# resumable-shard-io-go

A local, resumable, bounded-concurrency sharded read + verify pipeline.
Multiple local sources with different access capabilities are partitioned
into independent shards, read concurrently under explicit ceilings,
verified, merged into one deterministic output file, and durably
checkpointed — so cancellation, process kill and restart never lose,
duplicate or silently mix data generations.

- **Deterministic**: sources are concatenated in declaration order. Each
  shard lives at a fixed output offset, shards never span sources, and
  completion order is irrelevant. The merged bytes and their SHA-256 are
  exactly those of a strictly serial read of all sources.
- **Resumable & idempotent**: every confirmed shard is fsynced, read back
  and then recorded atomically. Restart re-verifies claimed output regions
  before reusing them; duplicate submissions and out-of-order completion
  are no-ops.
- **Explainable failures**: corruption, incompatible versions, source
  mismatch, permission denial, source mutation and cancellation each have a
  distinct sentinel error and a distinct log line naming the decision
  basis (shard range, checkpoint version, observed vs. bound fingerprint).

## Concepts

| Term        | Meaning                                                                 |
|-------------|-------------------------------------------------------------------------|
| `Plan`      | ordered source list + `ShardSize`; validated and hashed into a plan ID |
| `Source`    | one local backend, random-access (`ra`) or sequential (`seq`)          |
| `Shard`     | `[Offset, Offset+Length)` of the concatenated stream; one source only  |
| generation  | a source fingerprint `{size, mtime_ns, path}` the run is bound to      |
| checkpoint  | durable record of confirmed shard digests (see format below)           |
| confirmation| output region fsynced → readback matches read hash → atomic save       |

Two built-in local sources are provided (`FileSource` for both kinds):

- **Random-access** (`NewRandomAccessFileSource`): each shard opens its own
  descriptor and `ReadAt`s its range. Multiple shards may be in flight up
  to the source's own `MaxConcurrency`.
- **Sequential** (`NewSequentialFileSource`): one descriptor per stream;
  the pipeline processes that source's shards strictly in offset order
  with a single in-flight reader (its ceiling is always 1).

## Checkpoint format and compatibility

Checkpoints are one JSON file written **atomically**
(temp file in the checkpoint directory → `fsync` → `rename` → directory
`fsync`). Readers observe either the previous complete file or the new
complete file, never a torn write. Layout (version 1):

```json
{
  "magic": "shardio-checkpoint",
  "version": 1,
  "payload": {
    "version": 1,
    "plan_id": "<sha256 hex of plan + fingerprints>",
    "output_path": "/abs/path/out.bin",
    "shard_size": 4096,
    "total_size": 123456,
    "shard_count": 31,
    "fingerprints": {
      "a": {"Name":"a","Size":100000,"Stamp":1790219455811743931,"Source":"/p/a"}
    },
    "completed": {
      "0": "<lowercase hex sha256 of verified shard 0 bytes>",
      "1": "..."
    },
    "finished": false
  },
  "checksum": "<sha256 hex of the compact payload JSON>"
}
```

The envelope `checksum` authenticates the payload; payload bytes are
verified exactly, so truncation or any bit flip cannot pass.

Compatibility rules (strict — never silently reset or skip):

1. **Not parseable JSON / bad magic / bad checksum hex / checksum mismatch
   / structurally implausible payload** →
   `ErrCheckpointCorrupt`.
2. Parses and authenticates, but envelope or payload `version` is not
   supported by this binary → `ErrCheckpointVersion`. To change the layout
   incompatibly, bump `checkpointVersion`; old files then fail with this
   error instead of being misread.
3. Same version, but any of `output_path`, `shard_size`, `shard_count`,
   `total_size`, source set, source fingerprints or `plan_id` differ from
   the current plan → `ErrCheckpointMismatch`, with one line per
   differing field (e.g. observed vs. checkpoint size/stamp).
4. Checkpoints contain **no in-flight work**: a shard exists in
   `completed` only after its output region was fsynced and proven by
   readback. A crash between fsync and save is indistinguishable from a
   crash before the write — the shard is simply reprocessed on resume, and
   its bytes are identical because the generation fingerprint must match.
5. On resume every claimed region is hashed from the actual output file.
   A region whose digest disagrees with the checkpoint (torn write,
   truncated file, external edit) is **reprocessed**, and the decision is
   logged as `reprocess (basis: output readback ... != checkpoint ...)`.
6. A finished, fully verified checkpoint re-run is a no-op
   (`Reused`/`Skipped`, `Processed=0`); sources are not re-read.

## Concurrency and back-pressure semantics

- **Two independent ceilings.** Global in-flight shards ≤ `Concurrency`
  (default `GOMAXPROCS`); additionally ≤ each source's `MaxConcurrency`.
  A source capped at 1 is serialized even when the global pool is large.
  (`TestConcurrencyCeilingsAreRespected` asserts the bound is both
  respected and actually reached.)
- **Sequential ordering gate.** Sequential sources are driven by a
  dedicated per-source loop in shard order, one open stream at a time.
  Random-access shards use a pooled task channel; both share the global
  permit pool.
- **Hard memory bound.** No shard-sized buffers are materialized. Bytes
  stream source → 32 KiB chunk buffer → fixed-offset `WriteAt` into the
  output file. Worst-case staging memory is
  `Concurrency × 32 KiB` regardless of shard size or total data size.
- **Back-pressure.** A slow destination stalls the owning worker's
  `WriteAt` while it holds both its global and per-source permits, so
  outstanding work stops being pulled — memory cannot grow under a slow
  target.
- **Cancellation.** Every read loop checks its context between chunks;
  `SIGINT`/`SIGTERM` and context deadline/cancel cause all workers,
  streams and file handles to be released promptly at a chunk boundary.
  A run that stops early reports `ErrCanceled`; durable confirmations
  survive, and the run is resumable. The test suite asserts bounded
  goroutine counts after cancel.

## Deterministic output model

1. Shards partition the *concatenation* of sources in declaration order;
   a source boundary always coincides with a shard boundary.
2. Writes are positional (`pwrite`-style `WriteAt`), never sequential
   appends, so out-of-order completion cannot interleave bytes.
3. A shard is confirmed only when
   pre-read fingerprint == post-read fingerprint,
   read hash == readback hash of the output region,
   and the region was `Sync()`ed before the atomic checkpoint save.
4. At completion: output size must equal planned size; every region digest
   must equal the checkpoint digest; and a whole-file SHA-256 is reported.
   The result is therefore byte-identical to serial concatenation.

## Error classification

| Sentinel (errors.Is)        | Meaning and intended reaction                                   |
|-----------------------------|-----------------------------------------------------------------|
| `ErrInvalidPlan`            | bad config: shard size ≤ 0, no/duplicate/empty sources, output path equals a source path, capability/interface mismatch |
| `ErrPermissionDenied`       | source/output open or stat failed with `os.ErrPermission`; fix permissions and re-run; nothing is confirmed |
| `ErrCheckpointCorrupt`      | unreadable/unauthenticated checkpoint (see rules 1)            |
| `ErrCheckpointVersion`      | generated by an incompatible version (rule 2)                  |
| `ErrCheckpointMismatch`     | valid checkpoint, different generation/plan (rule 3)           |
| `ErrSourceChanged`          | fingerprint taken before/after a shard read differs from the bound generation — the shard is not merged; the whole run stops |
| `ErrShardRejected`          | short read, readback mismatch, short write, output size/region mismatch at final verification; resumable |
| `ErrCanceled`               | context cancel or deadline; timely full shutdown, resumable    |

`shardpipe` maps the checkpoint/source/permission/cancel classes to
distinct non-zero exit codes (3=version, 4=corrupt, 5=mismatch,
6=permission, 7=source changed, 8=canceled, 1=other).

## Failure isolation policy

Policy: **fail-fast per shard, fail-stop per task, no mixed generations**.

- The first shard failure cancels the internal context, draining pending
  and in-flight shards; there is no automatic retry *within* a run, so a
  repeatedly failing source cannot produce partially repeated data and a
  success result.
- Confirmed shards stay durable and are reused on the next invocation;
  unconfirmed bytes may physically exist in the output file but are
  neither trusted nor counted — resume either proves them (then reuses) or
  overwrites them.
- A source mutated mid-run (`size`/`mtime` change around a read) aborts
  with `ErrSourceChanged`; restarting against the old checkpoint fails
  with `ErrCheckpointMismatch` until the checkpoint is intentionally
  discarded (delete it and start a new task against the new generation).
  It is never possible to merge some shards of generation N with some of
  generation N′.
- Permission failure on any source aborts before shard processing of that
  source and is classified distinctly; permission-sensitive tests are
  skipped when running as root.
- Re-running an identical task after success reads nothing: every shard is
  proven from the checkpoint plus output readback.

This behavior is reproducible because the rules are mechanical
(fingerprint equality, positional regions, digest equality) and all
decisions are logged.

## Logs

Every significant line names the shard index, owning source and
`range=[start,end)`; checkpoint lines name the checkpoint `version` and
plan hash; skip/reuse/reprocess/abort lines name the basis. Examples:

```
plan: version=1 shards=31 total=123456 shardSize=4096 fingerprints={...}
shard 7 source=a range=[28672,32768) srcOffset=28672 len=4096: start
shard 7 source=a range=[28672,32768): confirmed checksum=… bytes=4096 duplicate=false (basis: source-read hash equals post-write output readback; fsynced before checkpoint save)
resume: checkpoint version=1 plan=8de633e95ef9 completed=3/31 pending=28 decision=resume
shard 0 source=s range=[0,4096): reprocess (basis: output readback "…" != checkpoint "…": <nil>)
run aborted: shardio: source changed during processing: … ; confirmed=3/31 remain durable and resumable
run complete: shards=31 verified=31 total=123456 merged_checksum=…
```

## Usage

```go
run := shardio.NewRun(shardio.Options{
    Plan: shardio.Plan{
        ShardSize: 4096,
        Sources: []shardio.Source{
            shardio.NewRandomAccessFileSource("a", "/data/a.bin", 2),
            shardio.NewSequentialFileSource("b", "/data/b.bin"),
        },
    },
    OutputPath:  "/work/out.bin",
    Checkpoints: shardio.NewFileCheckpointStore("/work/out.ckpt.json"),
    Concurrency: 4,
    Logger:      logger,
})
rep, err := run.Run(ctx)
```

CLI:

```
go build ./cmd/shardpipe
shardpipe -output out.bin -checkpoint out.ckpt.json \
  -shard-size 4096 -concurrency 4 \
  name=ra:/path/to/random.bin name=seq:/path/to/stream.bin
```

Positional specs are `NAME=ra:PATH` or `NAME=seq:PATH`; sources are
concatenated in argument order. Re-invoking resumes (or is a no-op when
finished); `SIGINT`/`SIGTERM` stop promptly and durably.

## Local verification

```bash
# unit + race tests (all paths, repeated to shake out scheduling races)
go test -race -count=10 ./...

# plain suite
go test ./...

# static checks
go vet ./...
gofmt -l .    # must print nothing

# manual end-to-end
go build -o shardpipe ./cmd/shardpipe
cat a.bin b.bin > expected.bin
./shardpipe -output out.bin -checkpoint out.ckpt.json \
  a=ra:$PWD/a.bin b=seq:$PWD/b.bin
cmp out.bin expected.bin
./shardpipe -output out.bin -checkpoint out.ckpt.json \
  a=ra:$PWD/a.bin b=seq:$PWD/b.bin    # second run: processed=0
```

### Coverage map

| Requirement                       | Tests                                                                                      |
|-----------------------------------|--------------------------------------------------------------------------------------------|
| order/merge determinism           | `TestOutputMatchesSerialConcatenation`, `TestMixedSourcesCooperate`, `TestStressManyRunsRace` |
| bounded + per-source concurrency  | `TestConcurrencyCeilingsAreRespected`                                                      |
| sequential order / single stream  | `TestSequentialSourceIsProcessedInOrder`                                                   |
| resume / duplicate / kill         | `TestResumeAfterInterruptionIsIdempotent`, `TestCheckpointSurvivesKillBetweenFsyncAndSave` |
| torn output repair on resume      | `TestTornOutputRegionIsReprocessedOnResume`                                                |
| source change (both kinds)        | `TestSourceChangeDuringRunAborts`, `TestSequentialSourceChangeDuringRunAborts`             |
| corrupt/version/mismatch checkpoint| `TestCheckpointCorruptionCases`, `TestUnknownEnvelopeVersion`, `TestVerifyForResumeMismatches` |
| atomic save                       | `TestFileStoreAtomicSaveLeavesNoTemp`                                                      |
| permission denial                 | `TestPermissionDeniedIsClassified`, `TestSequentialPermissionDeniedIsClassified`, `TestFingerprintPermissionDenied` |
| cancel / deadline / shutdown      | `TestCancellationLeavesNoUnfinishedWork`, `TestDeadlineExceededIsCanceled`                 |
| plan validation                   | `TestInvalidPlans`, `TestDuplicateSourceNamesRejected`, `TestOutputOverlappingSourceRejected`, `TestEmptySourceProducesNoShards` |
