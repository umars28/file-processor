package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunWritesFileAndStats(t *testing.T) {
	out := filepath.Join(t.TempDir(), "sales.csv")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-rows", "100", "-invalid-rate", "0.2", "-seed", "5", "-out", out}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, stderr %s", code, stderr.String())
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var stats struct {
		Rows    int            `json:"rows"`
		Valid   int            `json:"valid"`
		Invalid map[string]int `json:"invalid"`
		Bytes   int64          `json:"bytes"`
	}
	if err := json.Unmarshal(stderr.Bytes(), &stats); err != nil {
		t.Fatalf("stats on stderr: %v (%s)", err, stderr.String())
	}
	if stats.Rows != 100 || stats.Bytes != int64(len(data)) || !strings.HasPrefix(string(data), "transaction_id,") {
		t.Fatalf("stats = %+v, file %d bytes", stats, len(data))
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing when -out is set", stdout.String())
	}
}

func TestRunToStdout(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-rows", "3"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != 4 {
		t.Fatalf("stdout has %d lines, want header plus 3 rows", lines)
	}
}

func TestRunErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-h"}, &stdout, &stderr); code != 0 {
		t.Fatalf("-h exit = %d", code)
	}
	if code := run([]string{"-nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("bad flag exit = %d, want 2", code)
	}
	leftover := filepath.Join(t.TempDir(), "x.csv")
	if code := run([]string{"-invalid-rate", "2", "-out", leftover}, &stdout, &stderr); code != 1 {
		t.Fatalf("invalid rate exit = %d, want 1", code)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatalf("invalid options left an output file behind: %v", err)
	}
	if code := run([]string{"-out", filepath.Join(t.TempDir(), "missing", "x.csv")}, &stdout, &stderr); code != 1 {
		t.Fatalf("unwritable output exit = %d, want 1", code)
	}
}
