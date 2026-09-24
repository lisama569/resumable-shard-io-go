package shardio

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func sampleCheckpoint() *Checkpoint {
	return &Checkpoint{
		Version:   checkpointVersion,
		TaskID:    "task-1",
		Output:    "/tmp/out.bin",
		ShardSize: 64,
		Sources: []SourceState{
			{ID: "a", Kind: RandomAccess, MaxConcurrency: 2,
				Fingerprint: Fingerprint{Size: 128, Hash: "h-a", Mod: "t0"}},
			{ID: "b", Kind: Sequential, MaxConcurrency: 1,
				Fingerprint: Fingerprint{Size: 64, Hash: "h-b", Mod: "t1"}},
		},
		Completed: map[int]CompletedShard{
			0: {Index: 0, SourceID: "a", Start: 0, End: 64, Checksum: "c0", Bytes: 64},
		},
	}
}

func TestCheckpoint_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	cp := sampleCheckpoint()
	if err := cp.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.TaskID != cp.TaskID || got.ShardSize != cp.ShardSize || len(got.Sources) != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if !got.IsCompleted(0) || got.IsCompleted(1) {
		t.Fatal("IsCompleted wrong")
	}
	if got.Completed[0].Checksum != "c0" {
		t.Fatal("completed payload mismatch")
	}
}

func TestCheckpoint_MissingFileReturnsNil(t *testing.T) {
	got, err := LoadCheckpoint(filepath.Join(t.TempDir(), "absent"))
	if err != nil || got != nil {
		t.Fatalf("want (nil,nil), got (%v,%v)", got, err)
	}
}

func TestCheckpoint_CorruptVariants(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	if err := sampleCheckpoint().Save(good); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"not json":     []byte("{{{garbage"),
		"truncated":    raw[:len(raw)/2],
		"flipped byte": append(append([]byte{}, raw[:len(raw)-4]...), 'X', 'X', 'X', 'X'),
	}
	for name, data := range cases {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadCheckpoint(p)
		var e *Error
		if !errors.As(err, &e) || e.Kind != KindCheckpointCorrupt {
			t.Fatalf("%s: want KindCheckpointCorrupt, got %#v", name, err)
		}
	}

	// bad magic
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	env.Magic = "WRONG"
	bad, _ := json.Marshal(&env)
	p := filepath.Join(dir, "badmagic")
	_ = os.WriteFile(p, bad, 0o600)
	if _, err := LoadCheckpoint(p); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("bad magic: want corrupt, got %#v", err)
	}
}

func TestCheckpoint_VersionIncompatible(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	if err := sampleCheckpoint().Save(good); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(good)
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	env.Version = 999
	bad, _ := json.Marshal(&env)
	p := filepath.Join(dir, "future")
	_ = os.WriteFile(p, bad, 0o600)
	_, err := LoadCheckpoint(p)
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindCheckpointVersion {
		t.Fatalf("want KindCheckpointVersion, got %#v", err)
	}
	if !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("errors.Is ErrCheckpointVersion failed: %#v", err)
	}
}

func TestCheckpoint_ValidateAgainst(t *testing.T) {
	cp := sampleCheckpoint()
	states := cp.Sources

	if err := cp.ValidateAgainst("task-1", "/tmp/out.bin", 64, states); err != nil {
		t.Fatalf("matching config should validate: %v", err)
	}
	// 显式构造各类不匹配：
	if err := cp.ValidateAgainst("other-task", "/tmp/out.bin", 64, states); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("task-id mismatch not detected: %v", err)
	}
	if err := cp.ValidateAgainst("task-1", "/tmp/other.bin", 64, states); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("output mismatch not detected: %v", err)
	}
	if err := cp.ValidateAgainst("task-1", "/tmp/out.bin", 32, states); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("shard-size mismatch not detected: %v", err)
	}
	changed := append([]SourceState{}, states...)
	changed[0].Fingerprint.Hash = "h-a-CHANGED"
	if err := cp.ValidateAgainst("task-1", "/tmp/out.bin", 64, changed); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("source fingerprint mismatch not detected: %v", err)
	}
	dropped := states[:1]
	if err := cp.ValidateAgainst("task-1", "/tmp/out.bin", 64, dropped); !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("source-count mismatch not detected: %v", err)
	}
}

func kindIs(err error, kind ErrorKind) bool {
	var e *Error
	return errors.As(err, &e) && e.Kind == kind
}

func TestCheckpoint_MarkCompletedIdempotent(t *testing.T) {
	cp := sampleCheckpoint()
	cs := CompletedShard{Index: 3, SourceID: "b", Start: 0, End: 64, Checksum: "c3", Bytes: 64}
	if err := cp.MarkCompleted(cs); err != nil {
		t.Fatal(err)
	}
	if err := cp.MarkCompleted(cs); err != nil {
		t.Fatalf("identical duplicate must be idempotent, got %v", err)
	}
	conflict := cs
	conflict.Checksum = "different"
	err := cp.MarkCompleted(conflict)
	if !kindIs(err, KindCheckpointMismatch) {
		t.Fatalf("conflicting duplicate must fail with mismatch, got %#v", err)
	}
}

func TestCheckpoint_OutOfOrderCompletion(t *testing.T) {
	// 乱序完成（先 5 后 1）必须安全保存且可恢复。
	cp := sampleCheckpoint()
	cs5 := CompletedShard{Index: 5, SourceID: "b", Start: 0, End: 64, Checksum: "c5", Bytes: 64}
	cs1 := CompletedShard{Index: 1, SourceID: "a", Start: 64, End: 128, Checksum: "c1", Bytes: 64}
	if err := cp.MarkCompleted(cs5); err != nil {
		t.Fatal(err)
	}
	if err := cp.MarkCompleted(cs1); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "c")
	if err := cp.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsCompleted(5) || !got.IsCompleted(1) {
		t.Fatal("out-of-order completions lost")
	}
}

func TestCheckpoint_AtomicSaveLeavesNoTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c")
	for i := 0; i < 5; i++ {
		cp := sampleCheckpoint()
		cp.TaskID = fmt.Sprintf("task-%d", i)
		if err := cp.Save(path); err != nil {
			t.Fatal(err)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("atomic save leaked files: %v", entries)
	}
}
