package shardio

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
)

// SequentialFileSource 模拟只能顺序推进的本地来源：内部维护单调游标 next，
// ReadShard 仅接受 start == next 的请求（游标之前或之后均拒绝）。
// 这使“乱序完成”不会驱动来源回退，且单源并发天然被游标互斥限制为 1。
type SequentialFileSource struct {
	id   string
	path string
	file *os.File
	mu   sync.Mutex
	next int64
	base Fingerprint
}

// OpenSequentialFile 以只读方式打开顺序型来源并强制权限检查。
func OpenSequentialFile(id, path string) (*SequentialFileSource, error) {
	if id == "" {
		return nil, newError(KindInvalidArgument, "", "empty source id", nil)
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsPermission(err) {
			return nil, newError(KindPermission, id, "open read-only not permitted: "+path, err)
		}
		return nil, newError(KindSourceUnreadable, id, "open failed: "+path, err)
	}
	if info, statErr := f.Stat(); statErr == nil {
		if info.Mode().Perm()&0o400 == 0 {
			_ = f.Close()
			return nil, newError(KindPermission, id,
				fmt.Sprintf("source not readable (mode %o): %s", info.Mode().Perm(), path), nil)
		}
	}
	return &SequentialFileSource{id: id, path: path, file: f}, nil
}

// InitFingerprint 计算并缓存基准指纹。
func (s *SequentialFileSource) InitFingerprint(ctx context.Context) error {
	fp, err := computeFingerprint(ctx, s.path, s.id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.base = fp
	s.next = 0
	s.mu.Unlock()
	return nil
}

func (s *SequentialFileSource) ID() string { return s.id }

// ResumeAt 将顺序游标推进到已确认完成的连续前缀末尾（重启恢复时使用）。
// 语义上等同于顺序设备“卷过”已确认区间：该区间字节不被重新处理。
// 仅允许在任何 ReadShard 之前调用一次，且不得回退。
func (s *SequentialFileSource) ResumeAt(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next != 0 {
		return newError(KindInvalidArgument, s.id,
			fmt.Sprintf("ResumeAt(%d) called after reads began (cursor=%d)", offset, s.next), nil)
	}
	if offset < 0 || (s.base.Size != 0 && offset > s.base.Size) {
		return newError(KindInvalidArgument, s.id,
			fmt.Sprintf("ResumeAt(%d) out of range [0,%d]", offset, s.base.Size), nil)
	}
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return newError(KindSourceUnreadable, s.id, "spool/seek to resume offset failed", err)
	}
	s.next = offset
	return nil
}

func (s *SequentialFileSource) Kind() SourceKind    { return Sequential }
func (s *SequentialFileSource) MaxConcurrency() int { return 1 }
func (s *SequentialFileSource) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.Size
}

func (s *SequentialFileSource) Fingerprint(ctx context.Context) (Fingerprint, error) {
	return computeFingerprint(ctx, s.path, s.id)
}

func (s *SequentialFileSource) verifyUnchanged() error {
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
	mod := info.ModTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	if info.Size() != base.Size || mod != base.Mod {
		return newError(KindSourceChanged, s.id,
			fmt.Sprintf("source changed during read: want size=%d mod=%s, got size=%d mod=%s",
				base.Size, base.Mod, info.Size(), mod), nil)
	}
	return nil
}

// ReadShard 严格按游标顺序读取；start 必须等于当前游标。
func (s *SequentialFileSource) ReadShard(ctx context.Context, start, end int64) ([]byte, error) {
	if start < 0 || end < start {
		return nil, newError(KindInvalidArgument, s.id,
			fmt.Sprintf("bad range [%d,%d)", start, end), nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if start != s.next {
		return nil, newError(KindInvalidArgument, s.id,
			fmt.Sprintf("sequential source requires monotonic reads: cursor=%d, requested start=%d", s.next, start),
			nil)
	}
	if err := s.verifyUnchangedLocked(); err != nil {
		return nil, err
	}
	n := end - start
	buf := make([]byte, 0, n)
	rb := make([]byte, 32*1024)
	for int64(len(buf)) < n {
		if err := ctx.Err(); err != nil {
			return nil, classifyCtxErr(ctx, err)
		}
		want := n - int64(len(buf))
		if int64(len(rb)) > want {
			rb = rb[:want]
		} else {
			rb = make([]byte, want)
		}
		nr, err := s.file.Read(rb)
		buf = append(buf, rb[:nr]...)
		if err != nil {
			if err == io.EOF {
				if int64(len(buf)) == n {
					break
				}
				return nil, newError(KindSourceUnreadable, s.id,
					fmt.Sprintf("unexpected EOF at offset %d: want %d bytes, got %d",
						start+int64(len(buf)), n, len(buf)), nil)
			}
			if os.IsPermission(err) {
				return nil, newError(KindPermission, s.id, "read not permitted", err)
			}
			return nil, newError(KindSourceUnreadable, s.id, "sequential read failed", err)
		}
	}
	s.next = end
	if err := s.verifyUnchangedLocked(); err != nil {
		return nil, err
	}
	return buf, nil
}

// verifyUnchangedLocked 要求调用方持有 mu。
func (s *SequentialFileSource) verifyUnchangedLocked() error {
	base := s.base
	info, err := os.Stat(s.path)
	if err != nil {
		if os.IsPermission(err) {
			return newError(KindPermission, s.id, "stat not permitted during read", err)
		}
		return newError(KindSourceUnreadable, s.id, "stat failed during read", err)
	}
	mod := info.ModTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	if info.Size() != base.Size || mod != base.Mod {
		return newError(KindSourceChanged, s.id,
			fmt.Sprintf("source changed during read: want size=%d mod=%s, got size=%d mod=%s",
				base.Size, base.Mod, info.Size(), mod), nil)
	}
	return nil
}

func (s *SequentialFileSource) Close() error {
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
