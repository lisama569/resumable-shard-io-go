package shardio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validCheckpoint() *Checkpoint {
	return &Checkpoint{
		Version:    checkpointVersion,
		PlanID:     strings.Repeat("a", 64),
		OutputPath: "/tmp/out.bin",
		ShardSize:  1024,
		TotalSize:  2048,
		ShardCount: 2,
		Fingerprints: map[string]Fingerprint{
			"s": {Name: "s", Size: 2048, Stamp: 42, Source: "/tmp/s.bin"},
		},
		Completed: map[int]string{
			0: strings.Repeat("00", 32),
			1: strings.Repeat("11", 32),
		},
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	cp := validCheckpoint()
	raw, err := encodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCheckpoint(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.PlanID != cp.PlanID || got.ShardCount != 2 || len(got.Completed) != 2 {
		t.Fatalf("round trip lost data: %+v", got)
	}
}

func TestCheckpointCorruptionCases(t *testing.T) {
	good, err := encodeCheckpoint(validCheckpoint())
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"not json":          []byte("garbage"),
		"empty":             []byte{},
		"truncated":         good[:len(good)/2],
		"bad magic":         bytesReplaceMagic(good, "wrong"),
		"tampered payload":  bytesFlipPayloadByte(good),
		"checksum hex junk": bytesReplaceChecksum(good, "zz"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeCheckpoint(raw)
			if !errors.Is(err, ErrCheckpointCorrupt) {
				t.Fatalf("want ErrCheckpointCorrupt, got %v", err)
			}
		})
	}
}

func bytesReplaceMagic(raw []byte, _ string) []byte {
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		return []byte("garbage")
	}
	env["magic"] = "nope"
	out, _ := json.Marshal(env)
	return out
}

func bytesFlipPayloadByte(raw []byte) []byte {
	var env checkpointEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return []byte("garbage")
	}
	env.Payload[10] ^= 0xFF
	out, _ := json.Marshal(&env)
	return out
}

func bytesReplaceChecksum(raw []byte, _ string) []byte {
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		return []byte("garbage")
	}
	env["checksum"] = "zz"
	out, _ := json.Marshal(env)
	return out
}

func TestUnknownEnvelopeVersion(t *testing.T) {
	cp := validCheckpoint()
	cp.Version = 999
	payload, _ := json.Marshal(cp)
	env := struct {
		Magic    string          `json:"magic"`
		Version  int             `json:"version"`
		Payload  json.RawMessage `json:"payload"`
		Checksum string          `json:"checksum"`
	}{
		Magic:    checkpointMagic,
		Version:  999,
		Payload:  payload,
		Checksum: sha256SumHex(payload),
	}
	out, _ := json.Marshal(&env)
	_, err := decodeCheckpoint(out)
	if !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("want ErrCheckpointVersion, got %v", err)
	}
}

func TestShapeProblemsAreCorruption(t *testing.T) {
	cases := map[string]func(*Checkpoint){
		"completed out of range": func(cp *Checkpoint) { cp.Completed[99] = strings.Repeat("ab", 32) },
		"bad checksum hex":       func(cp *Checkpoint) { cp.Completed[0] = "short" },
		"missing fingerprints":   func(cp *Checkpoint) { cp.Fingerprints = nil },
		"finished incomplete":    func(cp *Checkpoint) { cp.Finished = true; delete(cp.Completed, 1) },
		"zero shard size":        func(cp *Checkpoint) { cp.ShardSize = 0 },
		"missing plan id":        func(cp *Checkpoint) { cp.PlanID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cp := validCheckpoint()
			mutate(cp)
			raw, err := encodeCheckpoint(cp)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			_, err = decodeCheckpoint(raw)
			if !errors.Is(err, ErrCheckpointCorrupt) && !errors.Is(err, ErrCheckpointVersion) {
				t.Fatalf("want corrupt/version error, got %v", err)
			}
		})
	}
}

func TestFileStoreMissingReturnsNil(t *testing.T) {
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "absent.json"))
	cp, err := store.Load(context.Background())
	if err != nil || cp != nil {
		t.Fatalf("want (nil,nil), got %v %v", cp, err)
	}
}

func TestFileStoreAtomicSaveLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "ckpt.json")
	store := NewFileCheckpointStore(path)
	cp := validCheckpoint()
	for i := 0; i < 5; i++ {
		cp.Completed[0] = strings.Repeat(string(rune('0'+i)), 64)
		if err := store.Save(context.Background(), cp); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".ckpt-") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
	got, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Completed[0] != cp.Completed[0] {
		t.Fatalf("last save not visible")
	}
}

func TestVerifyForResumeMismatches(t *testing.T) {
	data := []byte("hello-checkpoint-world-0123456789!!")
	f := newFixture(t)
	writeFile(t, filepath.Join(f.dir, "s.bin"), data)
	bp, err := buildPlan(context.Background(), Plan{
		ShardSize: 8,
		Sources:   []Source{NewRandomAccessFileSource("s", filepath.Join(f.dir, "s.bin"), 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := newCheckpoint(bp, Options{OutputPath: f.outPath, Plan: Plan{ShardSize: 8}})

	cases := map[string]func(*Checkpoint){
		"output path":      func(cp *Checkpoint) { cp.OutputPath = "/elsewhere.bin" },
		"shard size":       func(cp *Checkpoint) { cp.ShardSize = 9 },
		"plan id":          func(cp *Checkpoint) { cp.PlanID = strings.Repeat("f", 64) },
		"fingerprint size": func(cp *Checkpoint) { f2 := cp.Fingerprints["s"]; f2.Size++; cp.Fingerprints["s"] = f2 },
		"missing source":   func(cp *Checkpoint) { delete(cp.Fingerprints, "s") },
		"shard count":      func(cp *Checkpoint) { cp.ShardCount++ },
		"total size":       func(cp *Checkpoint) { cp.TotalSize++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cp := *base
			cp.Fingerprints = map[string]Fingerprint{}
			for k, v := range base.Fingerprints {
				cp.Fingerprints[k] = v
			}
			cp.Completed = map[int]string{}
			for k, v := range base.Completed {
				cp.Completed[k] = v
			}
			mutate(&cp)
			err := verifyForResume(&cp, bp, 8, f.outPath)
			if !errors.Is(err, ErrCheckpointMismatch) {
				t.Fatalf("want ErrCheckpointMismatch, got %v", err)
			}
		})
	}
	if err := verifyForResume(base, bp, 8, f.outPath); err != nil {
		t.Fatalf("identical checkpoint must match: %v", err)
	}
}

func sha256SumHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
