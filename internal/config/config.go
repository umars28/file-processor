package config

type Config struct {
	DatabaseURL string
	HTTPAddr    string
	DataDir     string
}

type MissingEnvError struct {
	Name string
}

func (e *MissingEnvError) Error() string {
	return "missing required environment variable " + e.Name
}

func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL: getenv("DATABASE_URL"),
		HTTPAddr:    getenv("HTTP_ADDR"),
		DataDir:     getenv("DATA_DIR"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, &MissingEnvError{Name: "DATABASE_URL"}
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	return cfg, nil
}
