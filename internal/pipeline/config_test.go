package pipeline

import (
	"errors"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() = %v, want nil", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*Config)
	}{
		{"zero readers", func(c *Config) { c.Readers = 0 }},
		{"negative workers", func(c *Config) { c.Workers = -1 }},
		{"zero batch size", func(c *Config) { c.BatchSize = 0 }},
		{"negative max errors", func(c *Config) { c.MaxErrorsPerFile = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfigValidateAllowsZeroMaxErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxErrorsPerFile = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}
