package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// SourceKind 表示来源的能力类型。
type SourceKind string

const (
	// RandomAccess 来源支持任意位置读取，可受限并发访问。
	RandomAccess SourceKind = "random_access"
	// Sequential 来源只能按字节偏移单调推进的顺序读取，单源串行。
	Sequential SourceKind = "sequential"
)

// Fingerprint 描述一次任务运行时对来源内容的版本指纹。
// 读取期间来源发生变化时，指纹复核必须失败而不是产出混合版本。
type Fingerprint struct {
	Size int64  `json:"size"`
	Hash string `json:"hash"` // 整个来源内容的 SHA-256（十六进制）
	Mod  string `json:"mod"`  // 修改时间，RFC3339Nano
}

// Shard 是可独立处理的字节半开区间 [Start, End)。
type Shard struct {
	SourceID string
	Index    int   // 全局分片序号（从 0 开始，决定输出顺序）
	Start    int64 // 在来源中的起始偏移
	End      int64 // 排他结束偏移
}

func (s Shard) Length() int64 { return s.End - s.Start }

// ShardResult 是一个分片读取与校验后的结果。
type ShardResult struct {
	Shard    Shard
	Data     []byte
	Checksum string // 对 Data 计算的 SHA-256（十六进制）
}

// Source 是本地来源的统一抽象。
type Source interface {
	// ID 为来源在任务内的唯一标识。
	ID() string
	// Kind 返回来源能力类型。
	Kind() SourceKind
	// Fingerprint 返回来源当前指纹；变化检测依据。
	Fingerprint(ctx context.Context) (Fingerprint, error)
	// ReadShard 读取分片区间并返回数据。顺序型来源要求 start 单调不减。
	ReadShard(ctx context.Context, start, end int64) ([]byte, error)
	// MaxConcurrency 返回该来源允许的并发上限（顺序型为 1）。
	MaxConcurrency() int
	// Size 返回打开时记录的来源字节数。
	Size() int64
	io.Closer
}

// PlanShards 按固定分片大小为来源列表生成全局有序的分片计划。
// 同一来源内的分片偏移严格递增；states 的顺序决定全局分片顺序。
func PlanShards(states []SourceState, shardSize int64) ([]Shard, error) {
	if shardSize <= 0 {
		return nil, newError(KindInvalidArgument, "", fmt.Sprintf("shard size must be positive, got %d", shardSize), nil)
	}
	if len(states) == 0 {
		return nil, newError(KindInvalidArgument, "", "no sources planned", nil)
	}
	var shards []Shard
	idx := 0
	for _, st := range states {
		if st.Fingerprint.Size < 0 {
			return nil, newError(KindInvalidArgument, st.ID, "negative source size", nil)
		}
		for off := int64(0); off < st.Fingerprint.Size; off += shardSize {
			end := off + shardSize
			if end > st.Fingerprint.Size {
				end = st.Fingerprint.Size
			}
			shards = append(shards, Shard{
				SourceID: st.ID,
				Index:    idx,
				Start:    off,
				End:      end,
			})
			idx++
		}
	}
	return shards, nil
}

// checksumBytes 返回 data 的 SHA-256 十六进制摘要。
func checksumBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// computeFingerprint 对本地文件计算内容指纹（大小 + 全量 SHA-256 + 修改时间）。
// 全量哈希保证“大小不变但内容被改写”的来源变化也能被识别。
func computeFingerprint(ctx context.Context, path, sourceID string) (Fingerprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsPermission(err) {
			return Fingerprint{}, newError(KindPermission, sourceID, "stat not permitted: "+path, err)
		}
		return Fingerprint{}, newError(KindSourceUnreadable, sourceID, "stat failed: "+path, err)
	}
	if info.IsDir() {
		return Fingerprint{}, newError(KindSourceUnreadable, sourceID, "source path is a directory: "+path, nil)
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsPermission(err) {
			return Fingerprint{}, newError(KindPermission, sourceID, "open not permitted: "+path, err)
		}
		return Fingerprint{}, newError(KindSourceUnreadable, sourceID, "open failed: "+path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := copyWithContext(ctx, h, f); err != nil {
		return Fingerprint{}, err
	}
	// 重新 stat 一次：哈希期间文件被替换/截断可在此被发现（mtime/size 变化）。
	info2, err := os.Stat(path)
	if err != nil {
		return Fingerprint{}, newError(KindSourceUnreadable, sourceID, "re-stat failed: "+path, err)
	}
	if info2.Size() != info.Size() || !info2.ModTime().Equal(info.ModTime()) {
		return Fingerprint{}, newError(KindSourceChanged, sourceID,
			fmt.Sprintf("source changed while fingerprinting: size %d->%d", info.Size(), info2.Size()), nil)
	}
	return Fingerprint{
		Size: info2.Size(),
		Hash: hex.EncodeToString(h.Sum(nil)),
		Mod:  info2.ModTime().UTC().Format(time.RFC3339Nano),
	}, nil
}

// copyWithContext 在 io.Copy 期间响应 ctx 取消。
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, classifyCtxErr(ctx, err)
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
			}
			written += int64(nw)
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}

// classifyCtxErr 将 context 错误映射为分类错误。
func classifyCtxErr(ctx context.Context, err error) error {
	if err == nil {
		err = ctx.Err()
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return newError(KindTimeout, "", "deadline exceeded", err)
	case errors.Is(err, context.Canceled):
		return newError(KindCanceled, "", "context canceled", err)
	default:
		return err
	}
}
