package shardio

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// RandomFileSource 是基于只读 *os.File（pread/ReadAt）的随机访问型本地来源。
// 多 goroutine 可在受限并发下同时读取不同区间；每次读取前后复核文件身份，
// 读取期间来源被替换或改写时返回 KindSourceChanged，绝不产出混合版本数据。
type RandomFileSource struct {
	id   string
	path string
	file *os.File
	max  int
	mu   sync.Mutex
	base Fingerprint // 打开时记录的基准指纹（含大小与全量哈希）
}

// OpenRandomFile 以只读方式打开随机访问型来源，并强制做一次权限与可读性检查。
func OpenRandomFile(id, path string, maxConcurrency int) (*RandomFileSource, error) {
	if id == "" {
		return nil, newError(KindInvalidArgument, "", "empty source id", nil)
	}
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsPermission(err) {
			return nil, newError(KindPermission, id, "open read-only not permitted: "+path, err)
		}
		return nil, newError(KindSourceUnreadable, id, "open failed: "+path, err)
	}
	// 以 O_RDONLY 打开后若元数据显示不可读，也视为权限不足（可复现的拒绝路径）。
	if info, statErr := f.Stat(); statErr == nil {
		if info.Mode().Perm()&0o400 == 0 {
			_ = f.Close()
			return nil, newError(KindPermission, id,
				fmt.Sprintf("source not readable (mode %o): %s", info.Mode().Perm(), path), nil)
		}
	}
	s := &RandomFileSource{id: id, path: path, file: f, max: maxConcurrency}
	return s, nil
}

// InitFingerprint 计算并缓存基准指纹。需在打开来源后、规划分片前调用。
func (s *RandomFileSource) InitFingerprint(ctx context.Context) error {
	fp, err := computeFingerprint(ctx, s.path, s.id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.base = fp
	s.mu.Unlock()
	return nil
}

func (s *RandomFileSource) ID() string          { return s.id }
func (s *RandomFileSource) Kind() SourceKind    { return RandomAccess }
func (s *RandomFileSource) MaxConcurrency() int { return s.max }
func (s *RandomFileSource) Size() int64         { s.mu.Lock(); defer s.mu.Unlock(); return s.base.Size }

func (s *RandomFileSource) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return computeFingerprint(ctx, s.path, s.id)
}

// verifyUnchanged 通过大小与修改时间快速复核文件未被改动。
// 与全量内容哈希配合：启动与收尾时做全量哈希，读取热路径只做轻量复核。
func (s *RandomFileSource) verifyUnchanged() error {
	s.mu.Lock()
	base := s.base
	s.mu.Unlock()
	info, err := os.Stat(s.path)
	if err != nil {
		if os.IsPermission(err) {
			return newError(KindPermission, s.id, "stat not permitted during read", err)
		}
		return newError(KindSourceUnreadable, s.id, "stat failed during read", err)
	}
	if info.Size() != base.Size || info.ModTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00") != base.Mod {
		return newError(KindSourceChanged, s.id,
			fmt.Sprintf("source changed during read: want size=%d mod=%s, got size=%d mod=%s",
				base.Size, base.Mod, info.Size(),
				info.ModTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")), nil)
	}
	return nil
}

// ReadAt 语义要求短读必须返回错误；本实现保证读满 end-start 或失败。
func (s *RandomFileSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	if start < 0 || end < start {
		return nil, newError(KindInvalidArgument, s.id,
			fmt.Sprintf("bad range [%d,%d)", start, end), nil)
	}
	if err := s.verifyUnchanged(); err != nil {
		return nil, err
	}
	n := end - start
	buf := make([]byte, n)
	if n == 0 {
		return buf, nil
	}
	var off int64
	for off < n {
		if err := ctx.Err(); err != nil {
			return nil, classifyCtxErr(ctx, err)
		}
		nr, err := s.file.ReadAt(buf[off:], start+off)
		off += int64(nr)
		if err != nil {
			// ReadAt 在读满前返回 EOF 属于异常（文件可能被截断）。
			if os.IsPermission(err) {
				return nil, newError(KindPermission, s.id, "read not permitted", err)
			}
			if changedErr := s.verifyUnchanged(); changedErr != nil {
				return nil, changedErr
			}
			return nil, newError(KindSourceUnreadable, s.id,
				fmt.Sprintf("read at %d failed after %d bytes", start+off, nr), err)
		}
	}
	if err := s.verifyUnchanged(); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *RandomFileSource) Close() error {
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
