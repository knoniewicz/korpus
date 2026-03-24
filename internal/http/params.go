package http

import (
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/knoniewicz/korpus/internal/constants"
	"github.com/knoniewicz/korpus/internal/schema"
)

const (
	FilterSuffixLTE  = "__lte"
	FilterSuffixGTE  = "__gte"
	FilterSuffixLT   = "__lt"
	FilterSuffixGT   = "__gt"
	FilterSuffixEQ   = "__eq"
	FilterSuffixNEQ  = "__neq"
	FilterSuffixLIKE = "__like"
	FilterSuffixIN   = "__in"
)

var suffixToOperator = map[string]schema.Operator{
	FilterSuffixLTE:  schema.OpLte,
	FilterSuffixGTE:  schema.OpGte,
	FilterSuffixLT:   schema.OpLt,
	FilterSuffixGT:   schema.OpGt,
	FilterSuffixEQ:   schema.OpEq,
	FilterSuffixNEQ:  schema.OpNeq,
	FilterSuffixLIKE: schema.OpLike,
	FilterSuffixIN:   schema.OpIn,
}

func parseEntityPath(r *http.Request) (service, entity string) {
	service = r.URL.Query().Get(constants.ParamService)
	entity = r.URL.Query().Get(constants.ParamEntity)
	return service, entity
}

func BuildQuery(r *http.Request, entitySchema *schema.Schema) *schema.QueryBuilder {
	qb := entitySchema.Query()
	applyFilters(r, qb, entitySchema.GetTableSchema())
	applySorting(r, qb)
	applyPagination(r, qb)
	applyColumnSelection(r, qb)
	applyResolves(r, qb)
	return qb
}

func applyFilters(r *http.Request, qb *schema.QueryBuilder, tableSchema *schema.TableSchema) {
	for key, values := range r.URL.Query() {
		if isReservedParam(key) || len(values) == 0 {
			continue
		}

		field, operator := parseFilterParam(key)
		if !slices.Contains(tableSchema.Columns, field) {
			continue
		}

		if operator == schema.OpIn {
			values := strings.Split(values[0], ",")
			for _, value := range values {
				qb.Where(field, operator, value)
			}
			continue
		}

		qb.Where(field, operator, values[0])
	}
}

func parseFilterParam(param string) (field string, op schema.Operator) {
	for suffix, operator := range suffixToOperator {
		if strings.HasSuffix(param, suffix) {
			return strings.TrimSuffix(param, suffix), operator
		}
	}
	return param, schema.OpEq
}

func applySorting(r *http.Request, qb *schema.QueryBuilder) {
	sortParam := r.URL.Query().Get(constants.ParamSort)
	if sortParam == "" {
		return
	}

	for _, field := range strings.Split(sortParam, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		qb.OrderBy(field)
	}
}

func applyPagination(r *http.Request, qb *schema.QueryBuilder) {
	if limit, err := strconv.Atoi(r.URL.Query().Get(constants.ParamLimit)); err == nil {
		qb.Limit(limit)
	}
	if offset, err := strconv.Atoi(r.URL.Query().Get(constants.ParamOffset)); err == nil {
		qb.Offset(offset)
	}
}

func applyColumnSelection(r *http.Request, qb *schema.QueryBuilder) {
	if fields := r.URL.Query().Get(constants.ParamFields); fields != "" {
		columns := strings.Split(fields, ",")
		qb.Select(columns...)
	}
}

func applyResolves(r *http.Request, qb *schema.QueryBuilder) {
	resolveParam := r.URL.Query().Get("resolve")
	if resolveParam == "" {
		return
	}

	includeChildren := r.URL.Query().Get("include_children") != "false"

	if depth, err := strconv.Atoi(r.URL.Query().Get("depth")); err == nil {
		qb.Depth(depth)
	}

	for _, table := range strings.Split(resolveParam, ",") {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		qb.Resolve(table, includeChildren)
	}
}

func isReservedParam(key string) bool {
	return key == constants.ParamLimit ||
		key == constants.ParamOffset ||
		key == constants.ParamFields ||
		key == constants.ParamSort ||
		key == constants.ParamResolve ||
		key == constants.ParamIncludeChildren ||
		key == constants.ParamDepth
}
