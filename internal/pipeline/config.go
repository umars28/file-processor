package pipeline

import (
	"errors"
	"fmt"
	"runtime"
)

var ErrInvalidConfig = errors.New("invalid pipeline config")

type Config struct {
	Readers          int
	Workers          int
	BatchSize        int
	MaxErrorsPerFile int
}

func DefaultConfig() Config {
	return Config{
		Readers:          4,
		Workers:          runtime.NumCPU(),
		BatchSize:        512,
		MaxErrorsPerFile: 100,
	}
}

func (c Config) Validate() error {
	var errs []error
	if c.Readers < 1 {
		errs = append(errs, fmt.Errorf("%w: readers must be at least 1, got %d", ErrInvalidConfig, c.Readers))
	}
	if c.Workers < 1 {
		errs = append(errs, fmt.Errorf("%w: workers must be at least 1, got %d", ErrInvalidConfig, c.Workers))
	}
	if c.BatchSize < 1 {
		errs = append(errs, fmt.Errorf("%w: batch size must be at least 1, got %d", ErrInvalidConfig, c.BatchSize))
	}
	if c.MaxErrorsPerFile < 0 {
		errs = append(errs, fmt.Errorf("%w: max errors per file must not be negative, got %d", ErrInvalidConfig, c.MaxErrorsPerFile))
	}
	return errors.Join(errs...)
}
