package shardio

import (
	"errors"
	"fmt"
	"os"
)

// Sentinel errors classify the reason a pipeline run failed.
// Use errors.Is to test for them; wrapping with fmt.Errorf("...: %w", err)
// preserves classification.
var (
	// ErrCheckpointCorrupt: checkpoint bytes cannot be parsed or fail
	// integrity checks (bad JSON, truncated, wrong envelope).
	ErrCheckpointCorrupt = sentinel("shardio: checkpoint corrupt")
	// ErrCheckpointVersion: checkpoint parses but its Version is not one
	// this binary can resume from.
	ErrCheckpointVersion = sentinel("shardio: checkpoint version incompatible")
	// ErrCheckpointMismatch: checkpoint is valid but belongs to a different
	// plan (shard size, source set/fingerprints or output path differ).
	ErrCheckpointMismatch = sentinel("shardio: checkpoint does not match current sources")
	// ErrPermissionDenied: a source or the output cannot be opened due to
	// filesystem permissions.
	ErrPermissionDenied = sentinel("shardio: permission denied")
	// ErrSourceChanged: a source's fingerprint changed between planning,
	// reading or verification — the data generation is no longer the one
	// the run was bound to.
	ErrSourceChanged = sentinel("shardio: source changed during processing")
	// ErrShardRejected: a shard's read bytes failed checksum verification or
	// its source reported a range error. The shard may be retried in a new
	// run; it is never merged.
	ErrShardRejected = sentinel("shardio: shard rejected")
	// ErrInvalidPlan: caller-supplied plan configuration is invalid.
	ErrInvalidPlan = sentinel("shardio: invalid plan")
	// ErrCanceled: the pipeline was stopped via context cancellation before
	// completion; no partial merge was published.
	ErrCanceled = sentinel("shardio: pipeline canceled")
)

type sentinel string

func (s sentinel) Error() string { return string(s) }

// classifyOSError maps filesystem permission errors to ErrPermissionDenied
// so callers can distinguish access problems from data problems. Other
// errors pass through unchanged.
func classifyOSError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	return err
}
