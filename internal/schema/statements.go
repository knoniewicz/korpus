package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/knoniewicz/korpus/internal/constants"
)

var sqlReservedKeywords = map[string]struct{}{
	"all": {}, "analyse": {}, "analyze": {}, "and": {}, "any": {}, "array": {}, "as": {}, "asc": {}, "asymmetric": {},
	"authorization": {}, "between": {}, "binary": {}, "both": {}, "case": {}, "cast": {}, "check": {}, "collate": {},
	"collation": {}, "column": {}, "concurrently": {}, "constraint": {}, "create": {}, "cross": {}, "current_catalog": {},
	"current_date": {}, "current_role": {}, "current_schema": {}, "current_time": {}, "current_timestamp": {},
	"current_user": {}, "default": {}, "deferrable": {}, "desc": {}, "distinct": {}, "do": {}, "else": {}, "end": {},
	"except": {}, "false": {}, "fetch": {}, "for": {}, "foreign": {}, "freeze": {}, "from": {}, "full": {}, "grant": {},
	"group": {}, "having": {}, "ilike": {}, "in": {}, "initially": {}, "inner": {}, "intersect": {}, "into": {},
	"is": {}, "isnull": {}, "join": {}, "lateral": {}, "leading": {}, "left": {}, "like": {}, "limit": {}, "localtime": {},
	"localtimestamp": {}, "natural": {}, "not": {}, "notnull": {}, "null": {}, "offset": {}, "on": {}, "only": {}, "or": {},
	"order": {}, "outer": {}, "overlaps": {}, "placing": {}, "primary": {}, "references": {}, "returning": {}, "right": {},
	"select": {}, "session_user": {}, "similar": {}, "some": {}, "symmetric": {}, "table": {}, "then": {}, "to": {},
	"trailing": {}, "true": {}, "union": {}, "unique": {}, "user": {}, "using": {}, "variadic": {}, "verbose": {}, "when": {},
	"where": {}, "window": {}, "with": {},
}

func isSQLReservedKeyword(identifier string) bool {
	_, exists := sqlReservedKeywords[strings.ToLower(identifier)]
	return exists
}

func isValidSQLIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}

	for i, r := range identifier {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}

		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}

	return true
}

func validateSQLIdentifier(identifier string, source string) error {
	if identifier == "" {
		return fmt.Errorf("invalid SQL identifier for %s: empty value", source)
	}

	if strings.TrimSpace(identifier) != identifier {
		return fmt.Errorf("invalid SQL identifier for %s: %q (must not contain leading or trailing whitespace)", source, identifier)
	}

	if !isValidSQLIdentifier(identifier) {
		return fmt.Errorf("invalid SQL identifier for %s: %q (must match [A-Za-z_][A-Za-z0-9_]*)", source, identifier)
	}

	if isSQLReservedKeyword(identifier) {
		return fmt.Errorf("invalid SQL identifier for %s: %q (reserved SQL keyword)", source, identifier)
	}

	return nil
}

func validateSQLTableIdentifier(identifier string, source string) error {
	if identifier == "" {
		return fmt.Errorf("invalid SQL table identifier for %s: empty value", source)
	}

	parts := strings.Split(identifier, ".")
	for idx, part := range parts {
		if err := validateSQLIdentifier(part, fmt.Sprintf("%s segment %d", source, idx+1)); err != nil {
			return fmt.Errorf("invalid SQL table identifier for %s: %q (%v)", source, identifier, err)
		}
	}

	return nil
}

func resolveServiceEntityKeyFromTableName(tableName string, source string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("invalid %s: empty table name", source)
	}

	index := strings.LastIndex(tableName, "_")
	if index <= 0 || index >= len(tableName)-1 {
		return "", fmt.Errorf("invalid %s: table %q must follow <service>_<entity> naming with non-empty service and entity (service may contain underscores)", source, tableName)
	}

	service := tableName[:index]
	entity := tableName[index+1:]

	if err := validateSQLIdentifier(service, fmt.Sprintf("%s service segment", source)); err != nil {
		return "", err
	}
	if err := validateSQLIdentifier(entity, fmt.Sprintf("%s entity segment", source)); err != nil {
		return "", err
	}

	return service + "." + entity, nil
}

// TableSchema holds prepared statements and metadata for a table
type TableSchema struct {
	TableName         string
	Columns           []string
	PrimaryKeyCol     string
	IsSessionScoped   bool
	InsertStmt        *sql.Stmt
	UpsertStmt        *sql.Stmt
	UpdateStmt        *sql.Stmt
	SoftDeleteStmt    *sql.Stmt
	ValuePool         sync.Pool
	UpsertParentStmts []UpsertParentStmt
	ChildTables       map[string]*ChildTableInfo
	ColumnTypes       map[string]string
}

type UpsertParentStmt struct {
	Stmt              *sql.Stmt
	KeyField          string
	ReturnField       string
	InjectIntoField   string
	StmtQuery         string
	AdditionalColumns []string
}

type ChildTableInfo struct {
	Table     string
	Key       string
	ParentKey string
}

func (s *Schema) GenerateStatements(db *sql.DB) error {
	if err := validateSQLTableIdentifier(s.TableName, "schema table name"); err != nil {
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
	columnNames := make([]string, 0, len(properties))
	upsertParents := make([]struct {
		StmtQuery         string
		KeyField          string
		ReturnField       string
		InjectIntoField   string
		AdditionalColumns []string
	}, 0)
	childTables := make(map[string]*ChildTableInfo)
	columnTypes := make(map[string]string)
	differentPrimaryKey := false
	primaryKeyCol := constants.FieldID
	isSessionScoped := false

	if s.Data[constants.SchemaKeyXSession] == true {
		isSessionScoped = true
	}

	for key, value := range properties {
		if err := validateSQLIdentifier(key, fmt.Sprintf("schema property %q", key)); err != nil {
			return err
		}

		if fieldDef, ok := value.(map[string]any); ok {
			sqlType := jsonToSqlType(fieldDef[constants.SchemaKeyType], fieldDef[constants.SchemaKeyFormat])

			if childRaw, exists := fieldDef[constants.SchemaKeyXChildEntity]; exists && childRaw != nil {
				child, ok := childRaw.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: expected object", constants.SchemaKeyXChildEntity, key)
				}

				childTableVal, ok := child["table"]
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: missing table", constants.SchemaKeyXChildEntity, key)
				}
				childTable, ok := childTableVal.(string)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: table must be a string", constants.SchemaKeyXChildEntity, key)
				}
				if err := validateSQLIdentifier(childTable, fmt.Sprintf("%s.table for field %q", constants.SchemaKeyXChildEntity, key)); err != nil {
					return err
				}

				childKey, err := resolveServiceEntityKeyFromTableName(childTable, fmt.Sprintf("%s.table for field %q", constants.SchemaKeyXChildEntity, key))
				if err != nil {
					return err
				}

				parentKeyVal, ok := child["parent_key_field"]
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: missing parent_key_field", constants.SchemaKeyXChildEntity, key)
				}
				parentKey, ok := parentKeyVal.(string)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: parent_key_field must be a string", constants.SchemaKeyXChildEntity, key)
				}
				if err := validateSQLIdentifier(parentKey, fmt.Sprintf("%s.parent_key_field for field %q", constants.SchemaKeyXChildEntity, key)); err != nil {
					return err
				}

				childTables[key] = &ChildTableInfo{Table: childTable, Key: childKey, ParentKey: parentKey}
				continue
			}

			if fieldDef[constants.SchemaKeyPrimaryKey] == true {
				differentPrimaryKey = true
				primaryKeyCol = key
			}

			if xLookupRaw, exists := fieldDef[constants.SchemaKeyXLookup]; exists && xLookupRaw != nil {
				xLookup, ok := xLookupRaw.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: expected object", constants.SchemaKeyXLookup, key)
				}

				tableVal, ok := xLookup["table"]
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: missing table", constants.SchemaKeyXLookup, key)
				}
				table, ok := tableVal.(string)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: table must be a string", constants.SchemaKeyXLookup, key)
				}
				if err := validateSQLTableIdentifier(table, fmt.Sprintf("%s.table for field %q", constants.SchemaKeyXLookup, key)); err != nil {
					return err
				}

				keyFieldVal, ok := xLookup["key_field"]
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: missing key_field", constants.SchemaKeyXLookup, key)
				}
				keyField, ok := keyFieldVal.(string)
				if !ok {
					return fmt.Errorf("invalid %s metadata for field %q: key_field must be a string", constants.SchemaKeyXLookup, key)
				}
				if err := validateSQLIdentifier(keyField, fmt.Sprintf("%s.key_field for field %q", constants.SchemaKeyXLookup, key)); err != nil {
					return err
				}

				var returnField string
				if rfVal, exists := xLookup["return_field"]; exists && rfVal != nil {
					rf, ok := rfVal.(string)
					if !ok {
						return fmt.Errorf("invalid %s metadata for field %q: return_field must be a string", constants.SchemaKeyXLookup, key)
					}
					returnField = rf
				} else {
					returnField = keyField
				}
				if err := validateSQLIdentifier(returnField, fmt.Sprintf("%s.return_field for field %q", constants.SchemaKeyXLookup, key)); err != nil {
					return err
				}

				var injectInto string
				if iiVal, exists := xLookup["inject_into"]; exists && iiVal != nil {
					ii, ok := iiVal.(string)
					if !ok {
						return fmt.Errorf("invalid %s metadata for field %q: inject_into must be a string", constants.SchemaKeyXLookup, key)
					}
					if err := validateSQLIdentifier(ii, fmt.Sprintf("%s.inject_into for field %q", constants.SchemaKeyXLookup, key)); err != nil {
						return err
					}
					injectInto = ii
				}

				var additionalCols []string
				if acVal, exists := xLookup["additional_columns"]; exists && acVal != nil {
					ac, ok := acVal.([]any)
					if !ok {
						return fmt.Errorf("invalid %s metadata for field %q: additional_columns must be an array of strings", constants.SchemaKeyXLookup, key)
					}
					for _, col := range ac {
						if colStr, ok := col.(string); ok {
							if err := validateSQLIdentifier(colStr, fmt.Sprintf("%s.additional_columns for field %q", constants.SchemaKeyXLookup, key)); err != nil {
								return err
							}
							additionalCols = append(additionalCols, colStr)
						} else {
							return fmt.Errorf("invalid %s metadata for field %q: additional_columns must contain only strings", constants.SchemaKeyXLookup, key)
						}
					}
				}

				columns := []string{keyField}
				placeholders := []string{"$1"}
				updateClauses := []string{fmt.Sprintf("%s = EXCLUDED.%s", keyField, keyField)}
				paramIdx := 2

				for _, col := range additionalCols {
					columns = append(columns, col)
					placeholders = append(placeholders, fmt.Sprintf("$%d", paramIdx))
					updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
					paramIdx++
				}

				columns = append(columns, constants.FieldRequestID)
				placeholders = append(placeholders, fmt.Sprintf("$%d", paramIdx))

				stmtQuery := fmt.Sprintf(
					"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING %s",
					table, strings.Join(columns, ", "), strings.Join(placeholders, ", "),
					keyField, strings.Join(updateClauses, ", "), returnField,
				)

				upsertParents = append(upsertParents, struct {
					StmtQuery         string
					KeyField          string
					ReturnField       string
					InjectIntoField   string
					AdditionalColumns []string
				}{StmtQuery: stmtQuery, KeyField: key, ReturnField: returnField, InjectIntoField: injectInto, AdditionalColumns: additionalCols})

				if injectInto != "" {
					continue
				}
			}

			columnNames = append(columnNames, key)
			columnTypes[key] = sqlType
		}
	}

	if !differentPrimaryKey {
		columnTypes[constants.FieldID] = "TEXT"
	}
	columnTypes[constants.FieldCreatedAt] = "TIMESTAMPTZ"
	columnTypes[constants.FieldUpdatedAt] = "TIMESTAMPTZ"
	columnTypes[constants.FieldRequestID] = "TEXT"

	columns := append(columnNames, constants.FieldCreatedAt, constants.FieldUpdatedAt, constants.FieldRequestID)
	for _, col := range columns {
		if err := validateSQLIdentifier(col, "generated table column list"); err != nil {
			return err
		}
	}
	if err := validateSQLIdentifier(primaryKeyCol, "primary key column"); err != nil {
		return err
	}

	insertPlaceholders := make([]string, 0, len(columns))
	for i := range columns {
		insertPlaceholders = append(insertPlaceholders, fmt.Sprintf("$%d", i+1))
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s;",
		s.TableName, strings.Join(columns, ", "), strings.Join(insertPlaceholders, ", "), primaryKeyCol)
	insertStmt, err := db.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %v", err)
	}

	updateSetClauses := make([]string, 0, len(columns)-1)
	for _, col := range columns {
		if col != primaryKeyCol {
			updateSetClauses = append(updateSetClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	upsertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING %s;",
		s.TableName, strings.Join(columns, ", "), strings.Join(insertPlaceholders, ", "),
		primaryKeyCol, strings.Join(updateSetClauses, ", "), primaryKeyCol)
	upsertStmt, err := db.Prepare(upsertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %v", err)
	}

	var updateStmt *sql.Stmt
	if isSessionScoped {
		updateQuery := fmt.Sprintf("UPDATE %s SET %s = $1, %s = $2 WHERE %s = $3",
			s.TableName, constants.FieldEndedAt, constants.FieldUpdatedAt, primaryKeyCol)
		updateStmt, err = db.Prepare(updateQuery)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %v", err)
		}
	}

	softDeleteQuery := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE %s = $2",
		s.TableName, constants.FieldDeletedAt, primaryKeyCol)
	softDeleteStmt, err := db.Prepare(softDeleteQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare soft delete statement: %v", err)
	}

	upsertParentStmts := make([]UpsertParentStmt, 0, len(upsertParents))
	for _, upsertParent := range upsertParents {
		stmt, err := db.Prepare(upsertParent.StmtQuery)
		if err != nil {
			return fmt.Errorf("failed to prepare upsert parent statement: %v", err)
		}
		upsertParentStmts = append(upsertParentStmts, UpsertParentStmt{
			Stmt:              stmt,
			KeyField:          upsertParent.KeyField,
			ReturnField:       upsertParent.ReturnField,
			InjectIntoField:   upsertParent.InjectIntoField,
			StmtQuery:         upsertParent.StmtQuery,
			AdditionalColumns: upsertParent.AdditionalColumns,
		})
	}

	s.tableSchema = &TableSchema{
		TableName:         s.TableName,
		Columns:           columns,
		PrimaryKeyCol:     primaryKeyCol,
		IsSessionScoped:   isSessionScoped,
		InsertStmt:        insertStmt,
		UpsertStmt:        upsertStmt,
		UpdateStmt:        updateStmt,
		SoftDeleteStmt:    softDeleteStmt,
		UpsertParentStmts: upsertParentStmts,
		ChildTables:       childTables,
		ColumnTypes:       columnTypes,
	}
	s.tableSchema.ValuePool = sync.Pool{
		New: func() any { return make([]interface{}, len(columns)) },
	}

	key := s.Service + "." + s.Entity
	registerTableSchema(key, s.tableSchema)

	return nil
}

var tableSchemaRegistry = make(map[string]*TableSchema)

func registerTableSchema(key string, ts *TableSchema) {
	tableSchemaRegistry[key] = ts
}

func GetTableSchemaByKey(key string) (*TableSchema, bool) {
	ts, ok := tableSchemaRegistry[key]
	return ts, ok
}

// parseValue converts database values to their proper Go types.
// Handles the case where PostgreSQL numeric types are returned as []byte
// by the pq driver, which would otherwise be base64 encoded in JSON.
func parseValue(value interface{}, colType *sql.ColumnType) interface{} {
	if value == nil {
		return nil
	}

	// Handle byte arrays that represent numeric values
	if byteVal, ok := value.([]byte); ok {
		strVal := string(byteVal)
		dbType := colType.DatabaseTypeName()

		// Try to parse as float for numeric types
		switch dbType {
		case "NUMERIC", "DECIMAL", "FLOAT4", "FLOAT8", "REAL", "DOUBLE PRECISION":
			if f, err := strconv.ParseFloat(strVal, 64); err == nil {
				return f
			}
		case "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT":
			if i, err := strconv.ParseInt(strVal, 10, 64); err == nil {
				return i
			}
		}

		// If not a numeric type or parsing failed, return as string
		return strVal
	}

	return value
}

func ScanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var entities []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan: %v", err)
		}

		entry := make(map[string]interface{})
		for i, col := range cols {
			entry[col] = parseValue(values[i], colTypes[i])
		}
		entities = append(entities, entry)
	}

	return entities, rows.Err()
}

type ChildEvent struct {
	Key       string
	ParentKey string
	Payload   map[string]interface{}
}

func (ts *TableSchema) Upsert(ctx context.Context, event map[string]interface{}, requestID string) (primaryKey string, children []ChildEvent, err error) {
	values := ts.ValuePool.Get().([]interface{})
	defer func() {
		for i := range values {
			values[i] = nil
		}
		ts.ValuePool.Put(values)
	}()

	now := time.Now().UTC().Format(time.RFC3339)
	payloadCols := len(ts.Columns) - 3 // exclude created_at, updated_at, request_id

	// Handle parent upserts (x-lookup)
	if ts.UpsertParentStmts != nil {
		for _, upsertParent := range ts.UpsertParentStmts {
			parentValue, exists := event[upsertParent.KeyField]
			if !exists || parentValue == nil {
				continue
			}

			if strVal, ok := parentValue.(string); ok && strVal == "" {
				continue
			}

			args := []interface{}{parentValue}
			for _, col := range upsertParent.AdditionalColumns {
				args = append(args, event[col])
			}
			args = append(args, requestID)

			var parentIDValue string
			if err := upsertParent.Stmt.QueryRowContext(ctx, args...).Scan(&parentIDValue); err != nil {
				return "", nil, fmt.Errorf("failed to upsert parent: %v", err)
			}

			if upsertParent.InjectIntoField != "" {
				event[upsertParent.InjectIntoField] = parentIDValue
			}
		}
	}

	// Build values array
	for i, key := range ts.Columns[:payloadCols] {
		value := event[key]
		if sqlType, ok := ts.ColumnTypes[key]; ok && sqlType == "JSONB" {
			// Marshal JSONB fields: handles both objects (map[string]interface{}) and arrays ([]interface{})
			if value != nil {
				if _, alreadyBytes := value.([]byte); !alreadyBytes {
					jsonBytes, err := json.Marshal(value)
					if err != nil {
						return "", nil, fmt.Errorf("failed to marshal JSONB field %s: %w", key, err)
					}
					value = jsonBytes
				}
			}
		}
		values[i] = value
	}
	values[payloadCols] = now   // created_at
	values[payloadCols+1] = now // updated_at
	values[payloadCols+2] = requestID

	// Execute upsert
	row := ts.UpsertStmt.QueryRowContext(ctx, values...)
	if err := row.Scan(&primaryKey); err != nil {
		return "", nil, err
	}

	// Collect child events
	for childKey, childTable := range ts.ChildTables {
		child, ok := event[childKey]
		if !ok {
			continue
		}
		childArray, ok := child.([]interface{})
		if !ok {
			continue
		}

		for _, childObject := range childArray {
			childObjectMap, ok := childObject.(map[string]interface{})
			if !ok {
				continue
			}
			childObjectMap[childTable.ParentKey] = primaryKey
			children = append(children, ChildEvent{
				Key:       childTable.Key,
				ParentKey: childTable.ParentKey,
				Payload:   childObjectMap,
			})
		}
	}

	return primaryKey, children, nil
}

func (ts *TableSchema) EndSession(ctx context.Context, primaryKeyValue string, endedAt string) error {
	if ts.UpdateStmt == nil {
		return fmt.Errorf("update statement not available (table is not x-session scoped)")
	}

	updatedAt := time.Now().UTC().Format(time.RFC3339)
	if _, err := ts.UpdateStmt.ExecContext(ctx, endedAt, updatedAt, primaryKeyValue); err != nil {
		return fmt.Errorf("failed to update: %v", err)
	}
	return nil
}

func (ts *TableSchema) SoftDelete(ctx context.Context, primaryKeyValue any) error {
	if ts.SoftDeleteStmt == nil {
		return fmt.Errorf("soft delete statement not available")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := ts.SoftDeleteStmt.ExecContext(ctx, now, primaryKeyValue)
	return err
}
