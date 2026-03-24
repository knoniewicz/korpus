package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/knoniewicz/korpus/internal/schema"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

func LoadExtensionSchemas(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || os.IsPermission(err) {
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		if !isExtensionSchema(root, path) {
			return nil
		}

		if err := schema.LoadSchemas(path); err != nil {
			if errors.Is(err, fs.ErrPermission) || os.IsPermission(err) {
				return nil
			}
			return err
		}

		return nil
	})
}

func isExtensionSchema(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}

	parts := strings.Split(filepath.ToSlash(filepath.Clean(rel)), "/")
	if len(parts) == 2 {
		return strings.HasSuffix(parts[len(parts)-1], ".json")
	}

	if len(parts) < 3 {
		return false
	}

	if parts[1] != "schemas" {
		return false
	}

	return strings.HasSuffix(parts[len(parts)-1], ".json")
}

func (db *DB) CreateTables(ctx context.Context) error {
	if db == nil || db.config == nil {
		return fmt.Errorf("database config is nil")
	}

	root := db.config.SchemaDir
	if root == "" {
		root = "./extensions"
	}

	err := LoadExtensionSchemas(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("schema directory does not exist: %s", root)
		}
		return fmt.Errorf("failed to load schemas from %s: %v", root, err)
	}

	schemas, err := schema.TopologicalSorted()
	if err != nil {
		return fmt.Errorf("failed to sort schemas: %v", err)
	}

	for _, s := range schemas {
		if err := s.GenerateTable(db.Conn); err != nil {
			return fmt.Errorf("failed to create table for %s.%s: %v", s.Service, s.Entity, err)
		}
		if err := s.GenerateStatements(db.Conn); err != nil {
			return fmt.Errorf("failed to prepare statements for %s.%s: %v", s.Service, s.Entity, err)
		}

		slog.Debug("registered schema", "service", s.Service, "entity", s.Entity, "table", s.TableName)
	}

	return nil
}
