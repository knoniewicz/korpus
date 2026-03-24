package env_loader

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

func Load() error {
	envPath, err := findEnvFile()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	if err := godotenv.Overload(envPath); err != nil {
		return fmt.Errorf("load %s: %w", envPath, err)
	}

	return nil
}

func findEnvFile() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}

	for {
		candidate := filepath.Join(dir, ".env")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("stat %s: %w", candidate, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
