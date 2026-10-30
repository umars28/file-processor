package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/umars28/file-processor/internal/gendata"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	o := gendata.Defaults()
	fs := flag.NewFlagSet("gendata", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.IntVar(&o.Rows, "rows", o.Rows, "data rows to write")
	fs.IntVar(&o.Outlets, "outlets", o.Outlets, "distinct outlets")
	fs.IntVar(&o.Products, "products", o.Products, "distinct products")
	fs.IntVar(&o.Days, "days", o.Days, "days covered, starting 2026-10-01 WIB")
	fs.Float64Var(&o.InvalidRate, "invalid-rate", o.InvalidRate, "share of rows written invalid, between 0 and 1")
	fs.Uint64Var(&o.Seed, "seed", o.Seed, "random seed; the same flags and seed give the same file")
	out := fs.String("out", "", "output file (default stdout)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if err := o.Validate(); err != nil {
		_, _ = fmt.Fprintf(stderr, "gendata: %v\n", err)
		return 1
	}
	w := stdout
	var f *os.File
	if *out != "" {
		var err error
		f, err = os.Create(filepath.Clean(*out))
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "gendata: %v\n", err)
			return 1
		}
		w = f
	}
	stats, err := gendata.Generate(w, o)
	if f != nil {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "gendata: %v\n", err)
		return 1
	}
	enc := json.NewEncoder(stderr)
	if err := enc.Encode(stats); err != nil {
		return 1
	}
	return 0
}
