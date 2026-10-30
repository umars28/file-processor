package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "fileproc")
	out, err := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func TestServeStopsOnSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX signals")
	}
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	bin := buildBinary(t)
	for name, sig := range map[string]syscall.Signal{"SIGINT": syscall.SIGINT, "SIGTERM": syscall.SIGTERM} {
		t.Run(name, func(t *testing.T) {
			stderr := &syncBuffer{}
			cmd := exec.CommandContext(t.Context(), bin, "serve", "-addr", "127.0.0.1:0")
			cmd.Env = append(os.Environ(), "DATABASE_URL=postgres://user@localhost/db")
			cmd.Stderr = stderr
			if err := cmd.Start(); err != nil {
				t.Fatalf("start: %v", err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()

			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(stderr.String(), "listening") {
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatalf("server did not start; stderr: %s", stderr.String())
				}
				time.Sleep(10 * time.Millisecond)
			}

			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatalf("signal: %v", err)
			}
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("exit = %v, want 0; stderr: %s", err, stderr.String())
				}
			case <-time.After(2 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("server did not exit within 2s of %s", name)
			}
			if !strings.Contains(stderr.String(), "stopped") {
				t.Fatalf("stderr = %q, want a stopped log", stderr.String())
			}
		})
	}
}
