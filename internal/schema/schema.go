package schema

import (
	"github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/constants"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type Schema struct {
	Service      string
	Entity       string
	TableName    string
	Path         string
	RawSchema    []byte
	JSONSchema   *jsonschema.Schema
	Data         map[string]any // parsed JSON
	ParentSchema *Schema
	ChildSchemas []*Schema
	tableSchema  *TableSchema
}

func (s *Schema) Properties() map[string]any {
	props, _ := s.Data[constants.SchemaKeyProperties].(map[string]any)
	return props
}

func (s *Schema) ChildEntityTables() []string {
	var children []string
	for _, field := range s.Properties() {
		fieldDef, ok := field.(map[string]any)
		if !ok {
			continue
		}
		if child, ok := fieldDef[constants.SchemaKeyXChildEntity].(map[string]any); ok {
			if table, ok := child["table"].(string); ok {
				children = append(children, table)
			}
		}
	}
	return children
}

func (s *Schema) ChildEntitySchemas() []*Schema {
	var schemas []*Schema
	for _, table := range s.ChildEntityTables() {
		if schema, ok := GetByTableName(table); ok {
			schemas = append(schemas, schema)
		}
	}
	return schemas
}

func (s *Schema) HasLookup() bool {
	for _, field := range s.Properties() {
		if fieldDef, ok := field.(map[string]any); ok {
			if _, ok := fieldDef[constants.SchemaKeyXLookup]; ok {
				return true
			}
		}
	}
	return false
}

func (s *Schema) HasForeignKey() bool {
	for _, field := range s.Properties() {
		if fieldDef, ok := field.(map[string]any); ok {
			if _, ok := fieldDef[constants.SchemaKeyForeignKey]; ok {
				return true
			}
		}
	}
	return false
}

func (s *Schema) HasChildEntity() bool {
	for _, field := range s.Properties() {
		if fieldDef, ok := field.(map[string]any); ok {
			if _, ok := fieldDef[constants.SchemaKeyXChildEntity]; ok {
				return true
			}
		}
	}
	return false
}

func (s *Schema) Validate(event *channel.Event, data any) error {
	return s.JSONSchema.Validate(data)
}

func (s *Schema) GetTableSchema() *TableSchema {
	return s.tableSchema
}
