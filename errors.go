package shardio

import (
	"errors"
)

// ErrorKind 对流水线中的失败进行可区分的分类。
type ErrorKind string

const (
	KindCanceled           ErrorKind = "canceled"
	KindTimeout            ErrorKind = "timeout"
	KindPermission         ErrorKind = "permission_denied"
	KindSourceChanged      ErrorKind = "source_changed"
	KindSourceUnreadable   ErrorKind = "source_unreadable"
	KindCheckpointCorrupt  ErrorKind = "checkpoint_corrupt"
	KindCheckpointVersion  ErrorKind = "checkpoint_version"
	KindCheckpointMismatch ErrorKind = "checkpoint_mismatch"
	KindChecksumMismatch   ErrorKind = "checksum_mismatch"
	KindInvalidArgument    ErrorKind = "invalid_argument"
)

// Sentinel errors，供 errors.Is 使用。
var (
	ErrCanceled           = errors.New("shardio: operation canceled")
	ErrPermissionDenied   = errors.New("shardio: permission denied")
	ErrCheckpointCorrupt  = errors.New("shardio: checkpoint corrupt")
	ErrCheckpointVersion  = errors.New("shardio: checkpoint version incompatible")
	ErrCheckpointMismatch = errors.New("shardio: checkpoint does not match current sources")
	ErrChecksumMismatch   = errors.New("shardio: checksum mismatch")
)

// Error 是带分类、来源标识与可解释依据的流水线错误。
type Error struct {
	Kind   ErrorKind
	Source string
	Reason string
	Cause  error
}

func (e *Error) Error() string {
	s := "shardio: " + string(e.Kind)
	if e.Source != "" {
		s += " [" + e.Source + "]"
	}
	if e.Reason != "" {
		s += ": " + e.Reason
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Cause }

// Is 允许 errors.Is(err, &Error{Kind: ...}) 按分类判定。
func (e *Error) Is(target error) bool {
	if t, ok := target.(*Error); ok {
		if t.Kind != "" && t.Kind != e.Kind {
			return false
		}
		if t.Source != "" && t.Source != e.Source {
			return false
		}
		return true
	}
	return false
}

func newError(kind ErrorKind, source, reason string, cause error) *Error {
	return &Error{Kind: kind, Source: source, Reason: reason, Cause: cause}
}
