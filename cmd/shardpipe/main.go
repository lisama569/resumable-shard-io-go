// Command shardpipe runs a resumable sharded read/verify pipeline against
// local files and writes the merged, verified result to an output file.
//
// Usage:
//
//	shardpipe -output out.bin -checkpoint out.ckpt \
//	  -shard-size 1048576 -concurrency 4 \
//	  name=ra:/path/to/file1 name=seq:/path/to/file2
//
// Each positional source spec is NAME=KIND:PATH where KIND is "ra"
// (random-access) or "seq" (sequential). Sources are concatenated in the
// order given. Re-running the same command resumes from the checkpoint;
// incompatible checkpoints fail with a classified error.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	shardio "github.com/lisama569/resumable-shard-io-go"
)

type stdLogger struct{ l *log.Logger }

func (s stdLogger) Printf(format string, args ...any) {
	s.l.Printf(format, args...)
}

func main() {
	output := flag.String("output", "", "merged output file (required)")
	ckpt := flag.String("checkpoint", "", "checkpoint file (required for resume)")
	shardSize := flag.Int("shard-size", 4*1024*1024, "maximum bytes per shard")
	concurrency := flag.Int("concurrency", 0, "global in-flight shard ceiling (default: GOMAXPROCS)")
	flag.Parse()

	if *output == "" || *ckpt == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "missing required flags or sources")
		flag.Usage()
		os.Exit(2)
	}

	var sources []shardio.Source
	for _, spec := range flag.Args() {
		name, kind, path, err := parseSpec(spec)
		if err != nil {
			fatal(err)
		}
		switch kind {
		case "ra":
			sources = append(sources, shardio.NewRandomAccessFileSource(name, path, concurrencyDefault(*concurrency)))
		case "seq":
			sources = append(sources, shardio.NewSequentialFileSource(name, path))
		default:
			fatal(fmt.Errorf("unknown source kind %q (want ra|seq)", kind))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	run := shardio.NewRun(shardio.Options{
		Plan:        shardio.Plan{ShardSize: *shardSize, Sources: sources},
		OutputPath:  *output,
		Checkpoints: shardio.NewFileCheckpointStore(*ckpt),
		Concurrency: *concurrency,
		Logger:      stdLogger{l: log.New(os.Stderr, "shardpipe ", log.LstdFlags|log.Lmicroseconds)},
	})
	rep, err := run.Run(ctx)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("ok shards=%d reused=%d processed=%d bytes=%d sha256=%s\n",
		rep.Shards, rep.Reused, rep.Processed, rep.TotalBytes, rep.MergedSHA256)
}

func parseSpec(spec string) (name, kind, path string, err error) {
	eq := strings.IndexByte(spec, '=')
	colon := strings.IndexByte(spec, ':')
	if eq <= 0 || colon < eq+1 {
		return "", "", "", fmt.Errorf("bad source spec %q, want NAME=KIND:PATH", spec)
	}
	name = spec[:eq]
	kind = strings.ToLower(spec[eq+1 : colon])
	path = spec[colon+1:]
	if name == "" || path == "" {
		return "", "", "", fmt.Errorf("bad source spec %q, want NAME=KIND:PATH", spec)
	}
	return name, kind, path, nil
}

func concurrencyDefault(c int) int {
	if c <= 0 {
		n, err := strconv.Atoi(os.Getenv("GOMAXPROCS"))
		if err == nil && n > 0 {
			return n
		}
		return 1
	}
	return c
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "shardpipe: %v\n", err)
	switch {
	case strings.Contains(err.Error(), string(shardio.ErrCheckpointVersion)):
		os.Exit(3)
	case strings.Contains(err.Error(), string(shardio.ErrCheckpointCorrupt)):
		os.Exit(4)
	case strings.Contains(err.Error(), string(shardio.ErrCheckpointMismatch)):
		os.Exit(5)
	case strings.Contains(err.Error(), string(shardio.ErrPermissionDenied)):
		os.Exit(6)
	case strings.Contains(err.Error(), string(shardio.ErrSourceChanged)):
		os.Exit(7)
	case strings.Contains(err.Error(), string(shardio.ErrCanceled)):
		os.Exit(8)
	}
	os.Exit(1)
}
