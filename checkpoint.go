package shardio

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	// checkpointVersion 是当前检查点格式版本。
	checkpointVersion = 1
	// checkpointMagic 标识文件归属，防止误用无关文件。
	checkpointMagic = "SHARDIO-CKPT"
)

// CompletedShard 记录一个已确认完成的分片。
type CompletedShard struct {
	Index    int    `json:"index"`
	SourceID string `json:"source_id"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Checksum string `json:"checksum"`
	Bytes    int    `json:"bytes"`
}

// SourceState 记录来源指纹与单源并发上限快照。
type SourceState struct {
	ID             string      `json:"id"`
	Kind           SourceKind  `json:"kind"`
	MaxConcurrency int         `json:"max_concurrency"`
	Fingerprint    Fingerprint `json:"fingerprint"`
}

// Checkpoint 是持久化的任务检查点（即封装格式中的 payload）。
type Checkpoint struct {
	Version   int                    `json:"version"`
	TaskID    string                 `json:"task_id"`
	Output    string                 `json:"output"`
	ShardSize int64                  `json:"shard_size"`
	Sources   []SourceState          `json:"sources"`
	Completed map[int]CompletedShard `json:"completed"`
	FinalHash string                 `json:"final_hash,omitempty"`

	mu sync.Mutex `json:"-"`
}

// envelope 是检查点文件的外层封装：magic 防误用，sha256 防损坏/半截写入。
type envelope struct {
	Magic   string          `json:"magic"`
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

// LoadCheckpoint 从文件读取并校验检查点；文件不存在时返回 (nil, nil)。
//
// 错误分类（可 errors.Is / Kind 区分）：
//   - KindCheckpointCorrupt：JSON 非法、magic 不符、摘要不匹配、字段缺失或半截写入；
//   - KindCheckpointVersion：封装或负载版本不是本实现支持的版本。
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if os.IsPermission(err) {
			return nil, newError(KindPermission, "", "checkpoint not readable: "+path, err)
		}
		return nil, newError(KindCheckpointCorrupt, "", "read checkpoint failed: "+path, err)
	}
	return parseCheckpoint(data)
}

func parseCheckpoint(data []byte) (*Checkpoint, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "checkpoint is not valid JSON", err)
	}
	if env.Magic != checkpointMagic {
		return nil, newError(KindCheckpointCorrupt, "",
			fmt.Sprintf("bad magic %q, want %q", env.Magic, checkpointMagic), ErrCheckpointCorrupt)
	}
	if len(env.Payload) == 0 {
		return nil, newError(KindCheckpointCorrupt, "", "empty payload", nil)
	}
	sum := sha256.Sum256(env.Payload)
	if env.SHA256 != hex.EncodeToString(sum[:]) {
		return nil, newError(KindCheckpointCorrupt, "",
			"checkpoint payload sha256 mismatch (file truncated, tampered, or partially written)", nil)
	}
	// 版本在解析 payload 之前判定，给出明确的版本不兼容失败。
	if env.Version != checkpointVersion {
		return nil, newError(KindCheckpointVersion, "",
			fmt.Sprintf("envelope version %d unsupported, want %d", env.Version, checkpointVersion),
			ErrCheckpointVersion)
	}
	var c Checkpoint
	if err := json.Unmarshal(env.Payload, &c); err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "payload unmarshal failed", err)
	}
	if c.Version != checkpointVersion {
		return nil, newError(KindCheckpointVersion, "",
			fmt.Sprintf("payload version %d unsupported, want %d", c.Version, checkpointVersion),
			ErrCheckpointVersion)
	}
	if c.Completed == nil {
		c.Completed = make(map[int]CompletedShard)
	}
	if err := c.sanity(); err != nil {
		return nil, err
	}
	return &c, nil
}

// sanity 做不依赖“当前来源”的内部一致性检查。
func (c *Checkpoint) sanity() error {
	if c.TaskID == "" {
		return newError(KindCheckpointCorrupt, "", "missing task id", nil)
	}
	if c.ShardSize <= 0 {
		return newError(KindCheckpointCorrupt, "", "invalid shard size", nil)
	}
	if len(c.Sources) == 0 {
		return newError(KindCheckpointCorrupt, "", "no sources in checkpoint", nil)
	}
	seen := make(map[string]bool, len(c.Sources))
	for i, st := range c.Sources {
		if st.ID == "" {
			return newError(KindCheckpointCorrupt, "", fmt.Sprintf("source #%d missing id", i), nil)
		}
		if seen[st.ID] {
			return newError(KindCheckpointCorrupt, "", "duplicate source id: "+st.ID, nil)
		}
		seen[st.ID] = true
		if st.Kind != RandomAccess && st.Kind != Sequential {
			return newError(KindCheckpointCorrupt, st.ID, "unknown source kind: "+string(st.Kind), nil)
		}
		if st.MaxConcurrency < 1 {
			return newError(KindCheckpointCorrupt, st.ID, "invalid max concurrency", nil)
		}
		if st.Fingerprint.Hash == "" {
			return newError(KindCheckpointCorrupt, st.ID, "missing source fingerprint hash", nil)
		}
	}
	for idx, cs := range c.Completed {
		if cs.Index != idx {
			return newError(KindCheckpointCorrupt, cs.SourceID,
				fmt.Sprintf("completed entry key %d != shard index %d", idx, cs.Index), nil)
		}
		if !seen[cs.SourceID] {
			return newError(KindCheckpointCorrupt, cs.SourceID,
				"completed shard references unknown source", nil)
		}
		if cs.Checksum == "" || cs.Bytes < 0 || cs.End < cs.Start {
			return newError(KindCheckpointCorrupt, cs.SourceID,
				fmt.Sprintf("malformed completed shard index=%d", cs.Index), nil)
		}
	}
	return nil
}

// ValidateAgainst 校验检查点与当前任务配置/来源指纹是否匹配。
// 不匹配明确归类为 KindCheckpointMismatch（区别于文件损坏与版本不兼容），
// 并指出是任务标识、输出、分片大小、来源集合还是某个来源指纹不一致。
func (c *Checkpoint) ValidateAgainst(taskID, output string, shardSize int64, states []SourceState) error {
	if c.TaskID != taskID {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("task id mismatch: checkpoint=%q current=%q", c.TaskID, taskID),
			ErrCheckpointMismatch)
	}
	if c.Output != output {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("output path mismatch: checkpoint=%q current=%q", c.Output, output),
			ErrCheckpointMismatch)
	}
	if c.ShardSize != shardSize {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("shard size mismatch: checkpoint=%d current=%d", c.ShardSize, shardSize),
			ErrCheckpointMismatch)
	}
	if len(c.Sources) != len(states) {
		return newError(KindCheckpointMismatch, "",
			fmt.Sprintf("source count mismatch: checkpoint=%d current=%d", len(c.Sources), len(states)),
			ErrCheckpointMismatch)
	}
	for i, want := range c.Sources {
		got := states[i]
		if want.ID != got.ID || want.Kind != got.Kind || want.MaxConcurrency != got.MaxConcurrency {
			return newError(KindCheckpointMismatch, got.ID,
				fmt.Sprintf("source #%d declaration mismatch: checkpoint={%s %s max=%d} current={%s %s max=%d}",
					i, want.ID, want.Kind, want.MaxConcurrency, got.ID, got.Kind, got.MaxConcurrency),
				ErrCheckpointMismatch)
		}
		if want.Fingerprint != got.Fingerprint {
			return newError(KindCheckpointMismatch, got.ID,
				fmt.Sprintf("source fingerprint mismatch: checkpoint={size=%d hash=%s mod=%s} current={size=%d hash=%s mod=%s}",
					want.Fingerprint.Size, want.Fingerprint.Hash, want.Fingerprint.Mod,
					got.Fingerprint.Size, got.Fingerprint.Hash, got.Fingerprint.Mod),
				ErrCheckpointMismatch)
		}
	}
	return nil
}

// MarkCompleted 幂等记录一个完成分片。
// 同一分片重复提交时，所有字段（含校验和）必须一致，否则明确报错，
// 绝不以第二次提交静默覆盖第一次确认的结果。
func (c *Checkpoint) MarkCompleted(cs CompletedShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Completed == nil {
		c.Completed = make(map[int]CompletedShard)
	}
	if old, ok := c.Completed[cs.Index]; ok {
		if old != cs {
			return newError(KindCheckpointMismatch, cs.SourceID,
				fmt.Sprintf("duplicate completion for shard %d conflicts: existing={%s [%d,%d) %s %dB} new={%s [%d,%d) %s %dB}",
					cs.Index, old.SourceID, old.Start, old.End, old.Checksum, old.Bytes,
					cs.SourceID, cs.Start, cs.End, cs.Checksum, cs.Bytes), nil)
		}
		return nil // 幂等：重复且一致
	}
	c.Completed[cs.Index] = cs
	return nil
}

// IsCompleted 返回分片是否已确认完成。
func (c *Checkpoint) IsCompleted(index int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.Completed[index]
	return ok
}

// SnapshotCompleted 返回已完成分片索引的快照集合。
func (c *Checkpoint) SnapshotCompleted() map[int]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[int]bool, len(c.Completed))
	for idx := range c.Completed {
		out[idx] = true
	}
	return out
}

// ForgetCompleted 撤销一个已确认分片的记录（修复时用于丢弃坏片）。
// 撤销不存在的分片是幂等空操作；同时清空 final_hash，因为输出整体等价性
// 已需要重新证明，旧终态哈希不得继续代表成品。
func (c *Checkpoint) ForgetCompleted(index int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.Completed[index]; !ok {
		return false
	}
	delete(c.Completed, index)
	c.FinalHash = ""
	return true
}

// UpdateSourceFingerprint 用当前指纹替换指定来源的快照（增量重算成功后使用）。
func (c *Checkpoint) UpdateSourceFingerprint(id string, fp Fingerprint) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.Sources {
		if c.Sources[i].ID == id {
			c.Sources[i].Fingerprint = fp
			return true
		}
	}
	return false
}

// Marshal 序列化为带 magic/版本/摘要的封装格式。
func (c *Checkpoint) Marshal() ([]byte, error) {
	c.Version = checkpointVersion
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "marshal payload failed", err)
	}
	sum := sha256.Sum256(payload)
	env := envelope{
		Magic:   checkpointMagic,
		Version: checkpointVersion,
		Payload: payload,
		SHA256:  hex.EncodeToString(sum[:]),
	}
	data, err := json.Marshal(&env)
	if err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "marshal envelope failed", err)
	}
	return data, nil
}

// Save 原子写入：临时文件 + fsync + rename，崩溃只可能留下旧文件或完整新文件。
func (c *Checkpoint) Save(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := c.marshalLocked()
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		if os.IsPermission(err) {
			return newError(KindPermission, "", "checkpoint tmp not writable: "+tmp, err)
		}
		return newError(KindCheckpointCorrupt, "", "create checkpoint tmp failed: "+tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return newError(KindCheckpointCorrupt, "", "write checkpoint tmp failed", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return newError(KindCheckpointCorrupt, "", "fsync checkpoint tmp failed", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return newError(KindCheckpointCorrupt, "", "close checkpoint tmp failed", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if os.IsPermission(err) {
			return newError(KindPermission, "", "rename checkpoint failed: "+path, err)
		}
		return newError(KindCheckpointCorrupt, "", "rename checkpoint failed: "+path, err)
	}
	// 尽量 fsync 所在目录，使 rename 在崩溃后持久。
	if dir, derr := os.Open(dirOf(path)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// marshalLocked 序列化但不加锁（供已持锁的 Save 使用）。
func (c *Checkpoint) marshalLocked() ([]byte, error) {
	c.Version = checkpointVersion
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "marshal payload failed", err)
	}
	sum := sha256.Sum256(payload)
	env := envelope{
		Magic:   checkpointMagic,
		Version: checkpointVersion,
		Payload: payload,
		SHA256:  hex.EncodeToString(sum[:]),
	}
	data, err := json.Marshal(&env)
	if err != nil {
		return nil, newError(KindCheckpointCorrupt, "", "marshal envelope failed", err)
	}
	return data, nil
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}

var _ io.Writer = (*os.File)(nil)
