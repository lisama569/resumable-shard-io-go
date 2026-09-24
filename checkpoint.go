package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// checkpointVersion is the only checkpoint generation understood by this
// binary. Bump it for incompatible layout changes; old checkpoints then
// fail resume with ErrCheckpointVersion rather than being silently reset.
const checkpointVersion = 1

// checkpointMagic scopes the file envelope so random JSON cannot be
// mistaken for a checkpoint.
const checkpointMagic = "shardio-checkpoint"

// Checkpoint is the persisted record of a run's progress.
//
// JSON layout (v1): an outer envelope carries magic/version and a SHA-256
// over the canonical payload, so truncation or bit rot is detected as
// ErrCheckpointCorrupt rather than treated as "no progress":
//
//	{
//	  "magic": "shardio-checkpoint",
//	  "version": 1,
//	  "payload": { ...Checkpoint fields..., "completed": {"0": "<sha256>"} },
//	  "checksum": "<sha256 of payload JSON>"
//	}
type Checkpoint struct {
	Version      int                    `json:"version"`
	PlanID       string                 `json:"plan_id"`
	OutputPath   string                 `json:"output_path"`
	ShardSize    int                    `json:"shard_size"`
	TotalSize    int64                  `json:"total_size"`
	ShardCount   int                    `json:"shard_count"`
	Fingerprints map[string]Fingerprint `json:"fingerprints"`
	// Completed maps shard index to the lowercase hex SHA-256 of the
	// verified shard bytes. Recording the checksum lets a resumed run prove
	// that already-merged output regions still match the bound generation.
	Completed map[int]string `json:"completed"`
	// Finished is set only after every shard is complete and the final
	// checkpoint is durable; re-running with Finished=true is a no-op.
	Finished bool `json:"finished"`
}

type checkpointEnvelope struct {
	Magic    string          `json:"magic"`
	Version  int             `json:"version"`
	Payload  json.RawMessage `json:"payload"`
	Checksum string          `json:"checksum"`
}

// CheckpointStore loads and atomically persists checkpoints.
type CheckpointStore interface {
	// Load returns (nil, nil) when no checkpoint exists yet.
	Load(ctx context.Context) (*Checkpoint, error)
	// Save must be atomic: readers either see the previous complete
	// checkpoint or the new complete one, never a torn write.
	Save(ctx context.Context, cp *Checkpoint) error
}

// FileCheckpointStore keeps checkpoints in a single JSON file, written
// atomically via temp-file + fsync + rename.
type FileCheckpointStore struct {
	Path string
}

// NewFileCheckpointStore creates a store at path. Parent directories are
// created lazily on Save.
func NewFileCheckpointStore(path string) *FileCheckpointStore {
	return &FileCheckpointStore{Path: path}
}

func (s *FileCheckpointStore) Load(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint %q: %w", s.Path, classifyOSError(err))
	}
	cp, err := decodeCheckpoint(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s.Path, err)
	}
	return cp, nil
}

func (s *FileCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := encodeCheckpoint(cp)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", classifyOSError(err))
	}
	tmp, err := os.CreateTemp(dir, ".ckpt-*")
	if err != nil {
		return fmt.Errorf("create checkpoint temp: %w", classifyOSError(err))
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write checkpoint: %w", classifyOSError(err))
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync checkpoint: %w", classifyOSError(err))
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close checkpoint: %w", classifyOSError(err))
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		cleanup()
		return fmt.Errorf("rename checkpoint: %w", classifyOSError(err))
	}
	// Best-effort directory fsync so the rename is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func encodeCheckpoint(cp *Checkpoint) ([]byte, error) {
	if cp.Version == 0 {
		cp.Version = checkpointVersion
	}
	payload, err := json.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal payload: %v", ErrCheckpointCorrupt, err)
	}
	sum := sha256.Sum256(payload)
	env := checkpointEnvelope{
		Magic:    checkpointMagic,
		Version:  cp.Version,
		Payload:  payload,
		Checksum: hex.EncodeToString(sum[:]),
	}
	raw, err := json.Marshal(&env)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal envelope: %v", ErrCheckpointCorrupt, err)
	}
	return raw, nil
}

// decodeCheckpoint classifies raw checkpoint bytes: parse/structure/
// integrity problems are ErrCheckpointCorrupt; a structurally sound file
// with an unknown version is ErrCheckpointVersion.
func decodeCheckpoint(raw []byte) (*Checkpoint, error) {
	cp, err := decodeCheckpointRaw(raw)
	if err == nil {
		return cp, nil
	}
	if errors.Is(err, ErrCheckpointVersion) {
		return nil, err
	}
	return nil, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
}

func decodeCheckpointRaw(raw []byte) (*Checkpoint, error) {
	var env checkpointEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("envelope is not valid JSON: %v", err)
	}
	if env.Magic != checkpointMagic {
		return nil, fmt.Errorf("bad magic %q", env.Magic)
	}
	want, err := hex.DecodeString(env.Checksum)
	if err != nil || len(want) != sha256.Size {
		return nil, fmt.Errorf("envelope checksum is not a %d-byte hex digest", sha256.Size)
	}
	sum := sha256.Sum256(env.Payload)
	if !hexEqual(sum[:], want) {
		return nil, fmt.Errorf("payload checksum mismatch (checkpoint tampered or truncated)")
	}
	if env.Version != checkpointVersion {
		return nil, fmt.Errorf("%w: checkpoint version %d, supported %d",
			ErrCheckpointVersion, env.Version, checkpointVersion)
	}
	var cp Checkpoint
	if err := json.Unmarshal(env.Payload, &cp); err != nil {
		return nil, fmt.Errorf("payload is not valid JSON: %v", err)
	}
	if err := cp.validateShape(); err != nil {
		return nil, err
	}
	return &cp, nil
}

// validateShape checks internal consistency independent of the current
// plan; anything structurally wrong is corruption.
func (cp *Checkpoint) validateShape() error {
	if cp.Version != checkpointVersion {
		return fmt.Errorf("%w: payload version %d, supported %d",
			ErrCheckpointVersion, cp.Version, checkpointVersion)
	}
	if cp.PlanID == "" || cp.OutputPath == "" {
		return fmt.Errorf("missing plan_id or output_path")
	}
	if cp.ShardSize <= 0 || cp.ShardCount < 0 || cp.TotalSize < 0 {
		return fmt.Errorf("implausible counters (shard_size=%d shard_count=%d total=%d)",
			cp.ShardSize, cp.ShardCount, cp.TotalSize)
	}
	if cp.Fingerprints == nil || len(cp.Fingerprints) == 0 {
		return fmt.Errorf("missing fingerprints")
	}
	if cp.Completed == nil {
		return fmt.Errorf("completed map is null")
	}
	for idx, sum := range cp.Completed {
		if idx < 0 || idx >= cp.ShardCount {
			return fmt.Errorf("completed shard %d outside [0,%d)", idx, cp.ShardCount)
		}
		b, err := hex.DecodeString(sum)
		if err != nil || len(b) != sha256.Size {
			return fmt.Errorf("completed shard %d has bad checksum %q", idx, sum)
		}
	}
	if cp.Finished && len(cp.Completed) != cp.ShardCount {
		return fmt.Errorf("finished checkpoint has %d/%d completed shards",
			len(cp.Completed), cp.ShardCount)
	}
	return nil
}

// verifyForResume checks a structurally valid checkpoint against the plan
// the caller is about to execute. Any mismatch is ErrCheckpointMismatch
// with details naming the exact field that differs — a resume never
// silently resets progress, and incompatible generations are never merged.
func verifyForResume(cp *Checkpoint, bp builtPlan, shardSize int, outputPath string) error {
	var problems []string
	if cp.OutputPath != outputPath {
		problems = append(problems, fmt.Sprintf("output_path: checkpoint %q != current %q",
			cp.OutputPath, outputPath))
	}
	if cp.ShardSize != shardSize {
		problems = append(problems, fmt.Sprintf("shard_size: checkpoint %d != current %d",
			cp.ShardSize, shardSize))
	}
	if cp.PlanID != bp.planID {
		problems = append(problems, "plan_id: checkpoint generation differs from current sources")
	}
	if cp.ShardCount != len(bp.shards) {
		problems = append(problems, fmt.Sprintf("shard_count: checkpoint %d != current %d",
			cp.ShardCount, len(bp.shards)))
	}
	if cp.TotalSize != bp.totalSize {
		problems = append(problems, fmt.Sprintf("total_size: checkpoint %d != current %d",
			cp.TotalSize, bp.totalSize))
	}
	if len(cp.Fingerprints) != len(bp.fingerprints) {
		problems = append(problems, fmt.Sprintf("source count: checkpoint %d != current %d",
			len(cp.Fingerprints), len(bp.fingerprints)))
	}
	for name, cur := range bp.fingerprints {
		old, ok := cp.Fingerprints[name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("source %q missing from checkpoint", name))
		case old != cur:
			problems = append(problems, fmt.Sprintf(
				"source %q changed: checkpoint {size=%d stamp=%d backend=%q} != current {size=%d stamp=%d backend=%q}",
				name, old.Size, old.Stamp, old.Source, cur.Size, cur.Stamp, cur.Source))
		}
	}
	for name := range cp.Fingerprints {
		if _, ok := bp.fingerprints[name]; !ok {
			problems = append(problems, fmt.Sprintf("source %q in checkpoint no longer present", name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w:\n  - %s", ErrCheckpointMismatch, joinLines(problems))
	}
	return nil
}

func joinLines(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += "\n  - "
		}
		out += s
	}
	return out
}

func hexEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
