package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIReplayAcrossProcesses(t *testing.T) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not available")
	}
	bin := filepath.Join(t.TempDir(), "simctl")
	build := exec.Command(goTool, "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	run := func(args ...string) ([]byte, error) {
		cmd := exec.Command(bin, args...)
		return cmd.CombinedOutput()
	}
	args := []string{"-seed", "7", "-ticks", "300", "-trace"}
	first, err := run(args...)
	if err != nil {
		t.Fatalf("first run: %v\n%s", err, first)
	}
	second, err := run(args...)
	if err != nil {
		t.Fatalf("second run: %v\n%s", err, second)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("same seed produced different CLI output\nfirst:\n%s\nsecond:\n%s", first, second)
	}
	if !bytes.Contains(first, []byte("trace-hash=")) || !bytes.Contains(first, []byte("status=ok")) {
		t.Fatalf("unexpected CLI output:\n%s", first)
	}
	third, err := run("-seed", "8", "-ticks", "300", "-trace")
	if err != nil {
		t.Fatalf("different seed run: %v\n%s", err, third)
	}
	if bytes.Equal(first, third) {
		t.Fatal("different seeds produced identical CLI output")
	}

	bad, err := run("-seed", "-1", "-ticks", "10")
	if err == nil {
		t.Fatalf("negative seed succeeded:\n%s", bad)
	}
	if !strings.Contains(string(bad), "seed must be >= 0") {
		t.Fatalf("negative seed error = %q", bad)
	}
}
