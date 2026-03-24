package schema

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/knoniewicz/korpus/internal/constants"
)

type Operator string

const (
	OpEq   Operator = "="
	OpNeq  Operator = "!="
	OpLt   Operator = "<"
	OpLte  Operator = "<="
	OpGt   Operator = ">"
	OpGte  Operator = ">="
	OpLike Operator = "LIKE"
	OpIn   Operator = "IN"
)

type SortDir string

const (
	Asc  SortDir = "ASC"
	Desc SortDir = "DESC"
)

type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
)

type QueryBuilder struct {
	schema          *Schema
	table           *TableSchema
	columns         []string
	conditions      []condition
	orderBy         []orderClause
	limitVal        int
	offsetVal       int
	args            []interface{}
	includeDeleted  bool
	joins           []join
	resolves        []resolve
	resolveDepth    int
	resolveAll      bool
	resolveChildren bool
}

type condition struct {
	field    string
	operator Operator
	value    interface{}
}

type orderClause struct {
	field string
	dir   SortDir
}

type join struct {
	table    string
	joinTo   string
	joinFrom string
	joinType JoinType
}

type resolve struct {
	table           string
	includeChildren bool
}

func (s *Schema) Query() *QueryBuilder {
	return &QueryBuilder{
		schema:       s,
		table:        s.GetTableSchema(),
		columns:      nil,
		conditions:   make([]condition, 0),
		orderBy:      make([]orderClause, 0),
		limitVal:     100,
		offsetVal:    0,
		args:         make([]interface{}, 0),
		resolves:     make([]resolve, 0),
		resolveDepth: 1,
	}
}

func (q *QueryBuilder) Select(columns ...string) *QueryBuilder {

	if columns[0] == "*" {
		q.columns = []string{"*"}
		return q
	}

	q.columns = filterValidColumns(columns, q.table.Columns)
	q.columns = append(q.columns, q.table.PrimaryKeyCol)
	return q
}

func (q *QueryBuilder) Where(field string, op Operator, value interface{}) *QueryBuilder {
	if !q.isValidColumn(field) {
		return q
	}
	q.conditions = append(q.conditions, condition{field: field, operator: op, value: value})
	return q
}

func (q *QueryBuilder) Join(table string, joinTo string, joinFrom string, joinType JoinType) *QueryBuilder {
	q.joins = append(q.joins, join{table: table, joinTo: joinTo, joinFrom: joinFrom, joinType: joinType})
	return q
}

func (q *QueryBuilder) Resolve(table string, includeChildren bool) *QueryBuilder {
	q.resolves = append(q.resolves, resolve{table: table, includeChildren: includeChildren})
	return q
}

func (q *QueryBuilder) ResolveAll(includeChildren bool) *QueryBuilder {
	q.resolveAll = true
	q.resolveChildren = includeChildren
	return q
}

func (q *QueryBuilder) Depth(n int) *QueryBuilder {
	if n > 0 {
		q.resolveDepth = n
	}
	return q
}

func (q *QueryBuilder) OrderBy(field string) *QueryBuilder {
	dir := Asc
	if strings.HasPrefix(field, "-") {
		field = field[1:]
		dir = Desc
	}
	if !q.isValidColumn(field) {
		return q
	}
	q.orderBy = append(q.orderBy, orderClause{field: field, dir: dir})
	return q
}

func (q *QueryBuilder) OrderByAsc(field string) *QueryBuilder {
	if !q.isValidColumn(field) {
		return q
	}
	q.orderBy = append(q.orderBy, orderClause{field: field, dir: Asc})
	return q
}

func (q *QueryBuilder) OrderByDesc(field string) *QueryBuilder {
	if !q.isValidColumn(field) {
		return q
	}
	q.orderBy = append(q.orderBy, orderClause{field: field, dir: Desc})
	return q
}

func (q *QueryBuilder) Limit(n int) *QueryBuilder {
	if n > 0 {
		q.limitVal = n
	}
	return q
}

func (q *QueryBuilder) Offset(n int) *QueryBuilder {
	if n >= 0 {
		q.offsetVal = n
	}
	return q
}

func (q *QueryBuilder) IncludeDeleted() *QueryBuilder {
	q.includeDeleted = true
	return q
}

func (q *QueryBuilder) Build() (string, []interface{}) {
	q.args = make([]interface{}, 0)
	argIdx := 1

	cols := q.columns
	if len(cols) == 0 {
		cols = q.table.Columns
		// NOTE: by default we dont save the primary key in the columns
		// i forgot why, but we use it to insert OFTEN, so it's better to just
		// hardcode the append here
		cols = append(cols, q.table.PrimaryKeyCol)
	}
	selectClause := strings.Join(cols, ", ")

	whereClauses := make([]string, 0)

	if !q.includeDeleted {
		whereClauses = append(whereClauses, fmt.Sprintf("%s.%s IS NULL", q.table.TableName, constants.FieldDeletedAt))
	}

	inConditions := make(map[string][]interface{})
	for _, cond := range q.conditions {
		if cond.operator == OpIn {
			inConditions[cond.field] = append(inConditions[cond.field], cond.value)
			continue
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s %s $%d", cond.field, cond.operator, argIdx))
		q.args = append(q.args, cond.value)
		argIdx++
	}

	for field, values := range inConditions {
		placeholders := make([]string, len(values))
		for i, val := range values {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			q.args = append(q.args, val)
			argIdx++
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ", ")))
	}

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	orderClause := q.buildOrderClause()
	joinClause := q.buildJoinClause()

	query := fmt.Sprintf("SELECT %s FROM %s %s %s %s LIMIT %d OFFSET %d",
		selectClause, q.table.TableName, joinClause, whereClause, orderClause, q.limitVal, q.offsetVal)

	slog.Debug("query built", "sql", query, "args", q.args)
	return query, q.args
}

func (q *QueryBuilder) Exec(ctx context.Context, db *sql.DB) ([]map[string]interface{}, error) {
	query, args := q.Build()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	results, err := ScanRows(rows)
	if err != nil {
		return nil, err
	}

	if (q.resolveAll || len(q.resolves) > 0) && len(results) > 0 {
		results = q.applyResolves(ctx, db, results)
	}

	return results, nil
}

func (q *QueryBuilder) applyResolves(ctx context.Context, db *sql.DB, entities []map[string]interface{}) []map[string]interface{} {
	if q.schema == nil {
		return entities
	}

	visited := make(map[string]bool)
	visited[q.schema.TableName] = true

	return q.resolveEntities(ctx, db, q.schema, entities, q.resolveDepth, visited)
}

func (q *QueryBuilder) resolveEntities(
	ctx context.Context,
	db *sql.DB,
	currentSchema *Schema,
	entities []map[string]interface{},
	depth int,
	visited map[string]bool,
) []map[string]interface{} {
	if depth <= 0 || len(entities) == 0 {
		return entities
	}

	// Build resolve sets from explicit resolves or resolveAll
	resolveSet := make(map[string]bool)
	includeChildrenSet := make(map[string]bool)

	if q.resolveAll {
		for _, rel := range currentSchema.GetForeignKeyRelations() {
			resolveSet[rel.ToTable] = true
		}
		if q.resolveChildren {
			for _, rel := range currentSchema.GetDependentRelations() {
				resolveSet[rel.FromTable] = true
				includeChildrenSet[rel.FromTable] = true
			}
		}
	} else {
		for _, r := range q.resolves {
			resolveSet[r.table] = true
			includeChildrenSet[r.table] = r.includeChildren
		}
	}

	basePK := currentSchema.GetPrimaryKey()
	result := make([]map[string]interface{}, 0, len(entities))

	for _, entity := range entities {
		nestedEntity := make(map[string]interface{})
		for k, v := range entity {
			nestedEntity[k] = v
		}

		// Resolve parent relations
		for _, rel := range currentSchema.GetForeignKeyRelations() {
			if !resolveSet[rel.ToTable] || visited[rel.ToTable] {
				continue
			}

			parentSchema, ok := GetByTableName(rel.ToTable)
			if !ok {
				continue
			}

			fkValue, ok := entity[rel.FromField]
			if !ok || fkValue == nil {
				continue
			}

			refKeyField := rel.ToKeyField
			if refKeyField == "" {
				refKeyField = parentSchema.GetPrimaryKey()
			}

			parentEntity, err := parentSchema.Query().
				Where(refKeyField, OpEq, fkValue).
				First(ctx, db)
			if err != nil {
				slog.Warn("failed to resolve parent", "table", rel.ToTable, "error", err)
				continue
			}

			if parentEntity != nil {
				// Recursively resolve parent's relations
				parentVisited := copyVisited(visited)
				parentVisited[rel.ToTable] = true
				resolved := q.resolveEntities(ctx, db, parentSchema, []map[string]interface{}{parentEntity}, depth-1, parentVisited)
				if len(resolved) > 0 {
					nestedEntity[rel.ToTable] = resolved[0]
				}
			}
		}

		// Resolve child relations
		basePKValue, hasPK := entity[basePK]
		if hasPK && basePKValue != nil {
			for _, rel := range currentSchema.GetDependentRelations() {
				if !resolveSet[rel.FromTable] || !includeChildrenSet[rel.FromTable] || visited[rel.FromTable] {
					continue
				}

				childSchema, ok := GetByTableName(rel.FromTable)
				if !ok {
					continue
				}

				childEntities, err := childSchema.Query().
					Where(rel.FromField, OpEq, basePKValue).
					Exec(ctx, db)
				if err != nil {
					slog.Warn("failed to resolve children", "table", rel.FromTable, "error", err)
					continue
				}

				if len(childEntities) > 0 {
					// Recursively resolve children's relations
					childVisited := copyVisited(visited)
					childVisited[rel.FromTable] = true
					nestedEntity[rel.FromTable] = q.resolveEntities(ctx, db, childSchema, childEntities, depth-1, childVisited)
				} else {
					nestedEntity[rel.FromTable] = []map[string]interface{}{}
				}
			}
		}

		result = append(result, nestedEntity)
	}

	return result
}

func copyVisited(visited map[string]bool) map[string]bool {
	copy := make(map[string]bool, len(visited))
	for k, v := range visited {
		copy[k] = v
	}
	return copy
}

func (q *QueryBuilder) First(ctx context.Context, db *sql.DB) (map[string]interface{}, error) {
	q.limitVal = 1
	results, err := q.Exec(ctx, db)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

func (q *QueryBuilder) buildJoinClause() string {
	if len(q.joins) == 0 {
		return ""
	}
	joinClauses := make([]string, 0)
	for _, join := range q.joins {
		joinClauses = append(joinClauses, fmt.Sprintf("%s JOIN %s ON %s = %s", join.joinType, join.table, join.joinTo, join.joinFrom))
	}
	return strings.Join(joinClauses, " ")
}

func (q *QueryBuilder) buildOrderClause() string {
	if len(q.orderBy) == 0 {
		return "ORDER BY " + q.table.TableName + "." + constants.FieldCreatedAt + " DESC"
	}

	parts := make([]string, len(q.orderBy))
	for i, o := range q.orderBy {
		parts[i] = fmt.Sprintf("%s %s", o.field, o.dir)
	}
	return "ORDER BY " + strings.Join(parts, ", ")
}

func (q *QueryBuilder) isValidColumn(field string) bool {
	for _, col := range q.table.Columns {
		if col == field {
			return true
		}
	}
	return false
}

func filterValidColumns(requested []string, available []string) []string {
	if len(requested) == 0 {
		return nil
	}
	availableSet := make(map[string]bool, len(available))
	for _, col := range available {
		availableSet[col] = true
	}
	filtered := make([]string, 0, len(requested))
	for _, col := range requested {
		if availableSet[col] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}
