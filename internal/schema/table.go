package schema

import (
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/knoniewicz/korpus/internal/constants"
)

var validSQLIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func validateSimpleSQLIdentifier(identifier string, description string) error {
	if !validSQLIdentifier.MatchString(identifier) {
		return fmt.Errorf("invalid %s: %q (must match ^[a-z_][a-z0-9_]*$)", description, identifier)
	}
	return nil
}

func validateTableName(name string) error {
	return validateSimpleSQLIdentifier(name, "table name")
}

type columnDef struct {
	name    string
	sqlType string
}

func (s *Schema) GenerateTable(db *sql.DB) error {
	if err := validateTableName(s.TableName); err != nil {
		return err
	}

	properties := s.Properties()
	if properties == nil {
		return fmt.Errorf("failed to get properties from schema")
	}

	required := []string{}
	if reqList, ok := s.Data[constants.SchemaKeyRequired].([]interface{}); ok {
		for _, req := range reqList {
			if req, ok := req.(string); ok {
				required = append(required, req)
			}
		}
	}

	columns := []string{
		constants.FieldRequestID + " TEXT NOT NULL",
		constants.FieldCreatedAt + " TIMESTAMPTZ DEFAULT NOW()",
		constants.FieldUpdatedAt + " TIMESTAMPTZ DEFAULT NOW()",
		constants.FieldDeletedAt + " TIMESTAMPTZ DEFAULT NULL",
	}

	columnDefs := []columnDef{
		{name: constants.FieldRequestID, sqlType: "TEXT"},
		{name: constants.FieldCreatedAt, sqlType: "TIMESTAMPTZ"},
		{name: constants.FieldUpdatedAt, sqlType: "TIMESTAMPTZ"},
		{name: constants.FieldDeletedAt, sqlType: "TIMESTAMPTZ"},
	}

	differentPrimaryKey := false

	slog.Debug("processing schema", "schema", s.Path)

	for key, value := range properties {
		if fieldDef, ok := value.(map[string]any); ok {
			if err := validateSQLIdentifier(key, fmt.Sprintf("column identifier for schema field %q", key)); err != nil {
				return err
			}

			sqlType := jsonToSqlType(fieldDef[constants.SchemaKeyType], fieldDef[constants.SchemaKeyFormat])

			if _, ok := fieldDef[constants.SchemaKeyXChildEntity].(map[string]any); ok {
				continue
			}

			additional := ""
			if fieldDef[constants.SchemaKeyPrimaryKey] == true {
				additional = " PRIMARY KEY DEFAULT gen_random_uuid()::text"
				differentPrimaryKey = true
			}

			if fieldDef[constants.SchemaKeyUnique] == true {
				additional += " UNIQUE"
			}

			if foreignKeyRaw, exists := fieldDef[constants.SchemaKeyForeignKey]; exists {
				foreignKey, ok := foreignKeyRaw.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: expected object, got %T", constants.SchemaKeyForeignKey, key, foreignKeyRaw)
				}

				table, err := requiredTableIdentifierField(foreignKey, "table", constants.SchemaKeyForeignKey, key)
				if err != nil {
					return err
				}

				foreignKeyCol, err := requiredIdentifierField(foreignKey, "key_field", constants.SchemaKeyForeignKey, key)
				if err != nil {
					return err
				}

				additional += fmt.Sprintf(" REFERENCES %s(%s)", table, foreignKeyCol)
			}

			var injectInto string
			if xLookupRaw, exists := fieldDef[constants.SchemaKeyXLookup]; exists {
				xLookup, ok := xLookupRaw.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: expected object, got %T", constants.SchemaKeyXLookup, key, xLookupRaw)
				}

				table, err := requiredTableIdentifierField(xLookup, "table", constants.SchemaKeyXLookup, key)
				if err != nil {
					return err
				}

				keyField, err := requiredIdentifierField(xLookup, "key_field", constants.SchemaKeyXLookup, key)
				if err != nil {
					return err
				}

				injectInto, err = optionalIdentifierField(xLookup, "inject_into", constants.SchemaKeyXLookup, key)
				if err != nil {
					return err
				}

				if _, err := optionalIdentifierField(xLookup, "return_field", constants.SchemaKeyXLookup, key); err != nil {
					return err
				}

				if injectInto == "" {
					additional += " REFERENCES " + table + "(" + keyField + ")"
				}
			}

			if injectInto != "" {
				continue
			}

			notNull := ""
			if slices.Contains(required, key) {
				notNull = " NOT NULL"
			}

			columns = append(columns, fmt.Sprintf("%s %s%s%s", key, sqlType, additional, notNull))
			columnDefs = append(columnDefs, columnDef{name: key, sqlType: sqlType})
		}
	}

	if !differentPrimaryKey {
		columns = append(columns, constants.FieldID+" TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text")
		columnDefs = append(columnDefs, columnDef{name: constants.FieldID, sqlType: "TEXT"})
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", s.TableName, strings.Join(columns, ", "))
	_, err := db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	if err := s.syncTableColumns(db, columnDefs); err != nil {
		return fmt.Errorf("failed to sync table columns: %v", err)
	}

	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_request_id ON %s(request_id);", s.TableName, s.TableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_created_at ON %s(created_at);", s.TableName, s.TableName),
	}

	for _, indexSQL := range indexes {
		if _, err := db.Exec(indexSQL); err != nil {
			slog.Warn("failed to create index", "error", err, "table", s.TableName)
		}
	}

	return nil
}

func requiredStringField(meta map[string]any, field string, extension string, schemaField string) (string, error) {
	raw, exists := meta[field]
	if !exists {
		return "", fmt.Errorf("invalid %s metadata for field %q: missing %q", extension, schemaField, field)
	}

	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("invalid %s metadata for field %q: %q must be a string, got %T", extension, schemaField, field, raw)
	}

	if value == "" {
		return "", fmt.Errorf("invalid %s metadata for field %q: %q cannot be empty", extension, schemaField, field)
	}

	return value, nil
}

func requiredIdentifierField(meta map[string]any, field string, extension string, schemaField string) (string, error) {
	value, err := requiredStringField(meta, field, extension, schemaField)
	if err != nil {
		return "", err
	}

	if err := validateSQLIdentifier(value, fmt.Sprintf("%s metadata for field %q: %q", extension, schemaField, field)); err != nil {
		return "", err
	}

	return value, nil
}

func requiredTableIdentifierField(meta map[string]any, field string, extension string, schemaField string) (string, error) {
	value, err := requiredStringField(meta, field, extension, schemaField)
	if err != nil {
		return "", err
	}

	if err := validateSQLTableIdentifier(value, fmt.Sprintf("%s metadata for field %q: %q", extension, schemaField, field)); err != nil {
		return "", err
	}

	return value, nil
}

func optionalStringField(meta map[string]any, field string, extension string, schemaField string) (string, error) {
	raw, exists := meta[field]
	if !exists || raw == nil {
		return "", nil
	}

	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("invalid %s metadata for field %q: optional %q must be a string, got %T", extension, schemaField, field, raw)
	}

	return value, nil
}

func optionalIdentifierField(meta map[string]any, field string, extension string, schemaField string) (string, error) {
	value, err := optionalStringField(meta, field, extension, schemaField)
	if err != nil {
		return "", err
	}

	if value == "" {
		return value, nil
	}

	if err := validateSQLIdentifier(value, fmt.Sprintf("%s metadata for field %q: %q", extension, schemaField, field)); err != nil {
		return "", err
	}

	return value, nil
}

func (s *Schema) syncTableColumns(db *sql.DB, expectedColumns []columnDef) error {
	if err := validateTableName(s.TableName); err != nil {
		return err
	}

	query := `SELECT column_name FROM information_schema.columns WHERE table_name = $1`
	rows, err := db.Query(query, s.TableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	existingCols := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return err
		}
		existingCols[colName] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, col := range expectedColumns {
		if err := validateSQLIdentifier(col.name, fmt.Sprintf("column identifier %q", col.name)); err != nil {
			return err
		}

		if !existingCols[col.name] {
			alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", s.TableName, col.name, col.sqlType)
			if _, err := db.Exec(alterSQL); err != nil {
				return fmt.Errorf("failed to add column %s: %v", col.name, err)
			}
			slog.Info("added missing column", "table", s.TableName, "column", col.name, "type", col.sqlType)
		}
	}
	return nil
}

func jsonToSqlType(jsonType interface{}, jsonFormat interface{}) string {
	if typeArray, ok := jsonType.([]interface{}); ok {
		for _, t := range typeArray {
			if t != constants.JSONTypeNull {
				jsonType = t
				break
			}
		}
	}

	if jsonFormat == constants.JSONFormatDateTime {
		return "TIMESTAMPTZ"
	}

	switch jsonType {
	case constants.JSONTypeString:
		return "TEXT"
	case constants.JSONTypeArray:
		return "JSONB"
	case constants.JSONTypeObject:
		return "JSONB"
	case constants.JSONTypeNumber:
		return "NUMERIC"
	case constants.JSONTypeBoolean:
		return "BOOLEAN"
	case constants.JSONFormatDateTime:
		return "TIMESTAMPTZ"
	}

	return "TEXT"
}
