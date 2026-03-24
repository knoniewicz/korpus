package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/knoniewicz/korpus/internal/channel"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func selectDraft(schemaURL string) *jsonschema.Draft {
	switch strings.TrimSuffix(schemaURL, "#") {
	case "https://json-schema.org/draft/2020-12/schema", "http://json-schema.org/draft/2020-12/schema":
		return jsonschema.Draft2020
	case "https://json-schema.org/draft/2019-09/schema", "http://json-schema.org/draft/2019-09/schema":
		return jsonschema.Draft2019
	case "https://json-schema.org/draft-07/schema", "http://json-schema.org/draft-07/schema":
		return jsonschema.Draft7
	case "https://json-schema.org/draft-06/schema", "http://json-schema.org/draft-06/schema":
		return jsonschema.Draft6
	case "https://json-schema.org/draft-04/schema", "http://json-schema.org/draft-04/schema":
		return jsonschema.Draft4
	default:
		return jsonschema.Draft2020
	}
}

func normalizedSchemaDocument(schemaData map[string]any) map[string]any {
	clone := make(map[string]any, len(schemaData))
	for key, value := range schemaData {
		if key == "$schema" {
			continue
		}
		clone[key] = value
	}
	return clone
}

func extractServiceAndEntity(path string) (service, entity string, err error) {
	parts := strings.Split(filepath.ToSlash(filepath.Clean(path)), "/")

	file := parts[len(parts)-1]
	if !strings.HasSuffix(file, ".json") {
		return "", "", fmt.Errorf("not a json schema: %s", path)
	}
	entity = strings.TrimSuffix(file, ".json")

	for i := 0; i < len(parts)-1; i++ {
		// Pattern: <root>/<service>/schemas/<entity>.json
		if i+1 < len(parts)-1 && parts[i+1] == "schemas" {
			service = parts[i]
			return service, entity, nil
		}

		// Pattern: <root>/schemas/<service>/<entity>.json
		if parts[i] == "schemas" && i+1 < len(parts)-1 {
			service = parts[i+1]
			return service, entity, nil
		}
	}

	return "", "", fmt.Errorf("could not infer service from path: %s", path)
}

func LoadSchemas(schemaDir string) error {
	err := filepath.Walk(schemaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		service, entity, err := extractServiceAndEntity(path)
		if err != nil {
			return fmt.Errorf("failed to get service and entity: %v", err)
		}
		tableName := fmt.Sprintf("%s_%s", service, entity)

		if err := channel.ValidateTableName(tableName); err != nil {
			return fmt.Errorf("failed to validate table name: %v", err)
		}

		schema, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read schema: %v", err)
		}

		var schemaData map[string]any
		if err := json.Unmarshal(schema, &schemaData); err != nil {
			return fmt.Errorf("failed to unmarshal schema: %v", err)
		}

		if skipKorpus, ok := schemaData["x-skip-korpus"].(bool); ok && skipKorpus {
			return nil
		}

		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(selectDraft(fmt.Sprint(schemaData["$schema"])))

		if err := compiler.AddResource(path, normalizedSchemaDocument(schemaData)); err != nil {
			return fmt.Errorf("failed to add schema resource: %v", err)
		}

		jsonSchema, err := compiler.Compile(path)
		if err != nil {
			return fmt.Errorf("failed to compile schema: %v", err)
		}

		schemaInfo := &Schema{
			Path:       path,
			Service:    service,
			Entity:     entity,
			TableName:  tableName,
			Data:       schemaData,
			RawSchema:  schema,
			JSONSchema: jsonSchema,
		}
		Register(schemaInfo)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to load schemas: %v", err)
	}
	return nil
}
