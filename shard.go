package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// Plan describes a deterministic partition of one or more sources into
// independently processable shards over a contiguous output byte range.
//
// Sources are concatenated in declaration order. Each shard belongs to
// exactly one source (a shard never spans a source boundary), so the merged
// byte stream is simply the sources concatenated in order; concurrent
// completion never changes output ordering.
type Plan struct {
	// ShardSize is the maximum number of bytes in one shard; must be > 0.
	ShardSize int
	// Sources are concatenated in slice order; names must be unique.
	Sources []Source
}

// Shard is one unit of work: [Offset, Offset+Length) of the concatenated
// source stream, backed by exactly one source.
type Shard struct {
	Index      int    // zero-based shard ordinal; also output position
	SourceName string // owning source's Name()
	Offset     int64  // absolute offset in the concatenated stream
	Length     int64  // bytes in this shard
	SrcOffset  int64  // offset within the owning source
}

// builtPlan holds the resolved shard list and the fingerprints observed
// while planning.
type builtPlan struct {
	shards       []Shard
	fingerprints map[string]Fingerprint
	totalSize    int64
	planID       string
}

// buildPlan validates the plan, fingerprints every source and partitions
// the concatenated stream into shards of at most plan.ShardSize bytes.
// Fingerprints taken here are the generation the run is bound to: any later
// mismatch is reported as ErrSourceChanged.
func buildPlan(ctx context.Context, plan Plan) (builtPlan, error) {
	var bp builtPlan
	if plan.ShardSize <= 0 {
		return bp, fmt.Errorf("%w: ShardSize must be > 0", ErrInvalidPlan)
	}
	if len(plan.Sources) == 0 {
		return bp, fmt.Errorf("%w: at least one source is required", ErrInvalidPlan)
	}

	bp.fingerprints = make(map[string]Fingerprint, len(plan.Sources))
	h := sha256.New()
	fmt.Fprintf(h, "v%d shardsize=%d sources=%d\n", checkpointVersion, plan.ShardSize, len(plan.Sources))

	var globalOffset int64
	for si, src := range plan.Sources {
		if src == nil {
			return bp, fmt.Errorf("%w: source %d is nil", ErrInvalidPlan, si)
		}
		name := src.Name()
		if name == "" {
			return bp, fmt.Errorf("%w: source %d has empty name", ErrInvalidPlan, si)
		}
		if _, dup := bp.fingerprints[name]; dup {
			return bp, fmt.Errorf("%w: duplicate source name %q", ErrInvalidPlan, name)
		}
		switch src.Kind() {
		case KindRandomAccess:
			if _, ok := src.(RandomAccessSource); !ok {
				return bp, fmt.Errorf("%w: source %q declares random access but does not implement RandomAccessSource", ErrInvalidPlan, name)
			}
		case KindSequential:
			if _, ok := src.(SequentialSource); !ok {
				return bp, fmt.Errorf("%w: source %q declares sequential but does not implement SequentialSource", ErrInvalidPlan, name)
			}
		default:
			return bp, fmt.Errorf("%w: source %q has unknown kind %d", ErrInvalidPlan, name, src.Kind())
		}
		if src.MaxConcurrency() < 1 {
			return bp, fmt.Errorf("%w: source %q reports MaxConcurrency < 1", ErrInvalidPlan, name)
		}

		fp, err := src.Fingerprint(ctx)
		if err != nil {
			return bp, fmt.Errorf("fingerprint %q: %w", name, classifyOSError(err))
		}
		if fp.Name == "" {
			fp.Name = name
		}
		bp.fingerprints[name] = fp
		fmt.Fprintf(h, "source[%d]=%s kind=%d size=%d stamp=%d backend=%q\n",
			si, name, src.Kind(), fp.Size, fp.Stamp, fp.Source)

		// Partition this source alone; shards never cross source boundaries.
		for off := int64(0); off < fp.Size; {
			length := int64(plan.ShardSize)
			if remaining := fp.Size - off; remaining < length {
				length = remaining
			}
			bp.shards = append(bp.shards, Shard{
				Index:      len(bp.shards),
				SourceName: name,
				Offset:     globalOffset + off,
				Length:     length,
				SrcOffset:  off,
			})
			off += length
		}
		globalOffset += fp.Size
	}
	bp.totalSize = globalOffset
	bp.planID = hex.EncodeToString(h.Sum(nil))
	return bp, nil
}

// planFingerprints is retained for callers that only need the generation
// fingerprint map (e.g. diagnostics); it re-runs buildPlan.
func planFingerprints(ctx context.Context, plan Plan) (map[string]Fingerprint, error) {
	bp, err := buildPlan(ctx, plan)
	if err != nil {
		return nil, err
	}
	return bp.fingerprints, nil
}

// canonicalFingerprintText renders a fingerprint map in source-name order;
// it is used for stable log lines and comparisons.
func canonicalFingerprintText(fps map[string]Fingerprint) string {
	names := make([]string, 0, len(fps))
	for n := range fps {
		names = append(names, n)
	}
	sort.Strings(names)
	var s string
	for _, n := range names {
		f := fps[n]
		s += fmt.Sprintf("%s{size=%d stamp=%d backend=%q};", n, f.Size, f.Stamp, f.Source)
	}
	return s
}
