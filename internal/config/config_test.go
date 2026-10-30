package config

import (
	"errors"
	"testing"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadMissingDatabaseURL(t *testing.T) {
	_, err := Load(env(nil))
	var missing *MissingEnvError
	if !errors.As(err, &missing) {
		t.Fatalf("Load() error = %v, want *MissingEnvError", err)
	}
	if missing.Name != "DATABASE_URL" {
		t.Fatalf("missing.Name = %q, want DATABASE_URL", missing.Name)
	}
	if got, want := err.Error(), "missing required environment variable DATABASE_URL"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"DATABASE_URL": "postgres://db"}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{DatabaseURL: "postgres://db", HTTPAddr: ":8080", DataDir: "./data"}
	if cfg != want {
		t.Fatalf("Load() = %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "postgres://db",
		"HTTP_ADDR":    "127.0.0.1:9000",
		"DATA_DIR":     "/data",
	}))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{DatabaseURL: "postgres://db", HTTPAddr: "127.0.0.1:9000", DataDir: "/data"}
	if cfg != want {
		t.Fatalf("Load() = %+v, want %+v", cfg, want)
	}
}
