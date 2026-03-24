package schema

import (
	"github.com/knoniewicz/korpus/internal/constants"
	"sync"
)

type ForeignKeyRelation struct {
	FromField  string
	ToTable    string
	ToKeyField string
	IsXLookup  bool
}

type DependentRelation struct {
	FromTable     string
	FromField     string
	ToKeyField    string
	IsChildEntity bool
}

var (
	incomingRefsIndex   = make(map[string][]DependentRelation)
	incomingRefsBuilt   bool
	incomingRefsIndexMu sync.RWMutex
)

func ensureIncomingRefsIndex() {
	incomingRefsIndexMu.RLock()
	built := incomingRefsBuilt
	incomingRefsIndexMu.RUnlock()

	if built {
		return
	}

	incomingRefsIndexMu.Lock()
	defer incomingRefsIndexMu.Unlock()

	// Double-check after acquiring write lock
	if incomingRefsBuilt {
		return
	}

	buildIncomingRefsIndex()
	incomingRefsBuilt = true
}

func InvalidateDependentsIndex() {
	incomingRefsIndexMu.Lock()
	defer incomingRefsIndexMu.Unlock()
	incomingRefsBuilt = false
	incomingRefsIndex = make(map[string][]DependentRelation)
}

func buildIncomingRefsIndex() {
	idx := make(map[string][]DependentRelation)
	all := All()

	pkByTable := make(map[string]string, len(all))
	for _, s := range all {
		if s.tableSchema != nil {
			pkByTable[s.TableName] = s.GetPrimaryKey()
		}
	}

	for _, fromSchema := range all {
		if fromSchema.tableSchema == nil {
			continue
		}
		fromTable := fromSchema.TableName
		props := fromSchema.Properties()

		for fieldName, raw := range props {
			def, ok := raw.(map[string]any)
			if !ok {
				continue
			}

			if toTable, toKey, ok := parseRef(def, constants.SchemaKeyForeignKey); ok {
				if toTable != "" && toTable != fromTable {
					if toKey == "" {
						toKey = pkByTable[toTable]
					}
					idx[toTable] = append(idx[toTable], DependentRelation{
						FromTable:     fromTable,
						FromField:     fieldName,
						ToKeyField:    toKey,
						IsChildEntity: false,
					})
				}
			}

			if toTable, toKey, ok := parseRef(def, constants.SchemaKeyXLookup); ok {
				if toTable != "" && toTable != fromTable {
					if toKey == "" {
						toKey = pkByTable[toTable]
					}
					idx[toTable] = append(idx[toTable], DependentRelation{
						FromTable:     fromTable,
						FromField:     fieldName,
						ToKeyField:    toKey,
						IsChildEntity: false,
					})
				}
			}
		}
	}

	incomingRefsIndex = idx
}

func parseRef(fieldDef map[string]any, key string) (table, keyField string, ok bool) {
	m, _ := fieldDef[key].(map[string]any)
	if m == nil {
		return "", "", false
	}
	t, _ := m["table"].(string)
	k, _ := m["key_field"].(string)
	return t, k, true
}

func (s *Schema) Dependencies() []string {
	seen := make(map[string]struct{})
	deps := make([]string, 0, 8)

	props := s.Properties()
	for _, raw := range props {
		def, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		if table, _, ok := parseRef(def, constants.SchemaKeyXLookup); ok && table != "" && table != s.TableName {
			if _, exists := seen[table]; !exists {
				seen[table] = struct{}{}
				deps = append(deps, table)
			}
		}

		if table, _, ok := parseRef(def, constants.SchemaKeyForeignKey); ok && table != "" && table != s.TableName {
			if _, exists := seen[table]; !exists {
				seen[table] = struct{}{}
				deps = append(deps, table)
			}
		}
	}

	return deps
}

func (s *Schema) Dependents() []string {
	ensureIncomingRefsIndex()

	incomingRefsIndexMu.RLock()
	rels := incomingRefsIndex[s.TableName]
	incomingRefsIndexMu.RUnlock()

	if len(rels) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(rels))
	out := make([]string, 0, len(rels))
	for _, r := range rels {
		if _, ok := seen[r.FromTable]; ok {
			continue
		}
		seen[r.FromTable] = struct{}{}
		out = append(out, r.FromTable)
	}
	return out
}

func (s *Schema) GetPrimaryKey() string {
	return s.tableSchema.PrimaryKeyCol
}

func (s *Schema) GetForeignKeyRelations() []ForeignKeyRelation {
	props := s.Properties()
	rels := make([]ForeignKeyRelation, 0, 8)

	for fieldName, raw := range props {
		def, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		if table, keyField, ok := parseRef(def, constants.SchemaKeyForeignKey); ok && table != "" && table != s.TableName {
			rels = append(rels, ForeignKeyRelation{
				FromField:  fieldName,
				ToTable:    table,
				ToKeyField: keyField,
				IsXLookup:  false,
			})
		}

		if table, keyField, ok := parseRef(def, constants.SchemaKeyXLookup); ok && table != "" && table != s.TableName {
			rels = append(rels, ForeignKeyRelation{
				FromField:  fieldName,
				ToTable:    table,
				ToKeyField: keyField,
				IsXLookup:  true,
			})
		}
	}

	return rels
}

func (s *Schema) GetDependentRelations() []DependentRelation {
	ensureIncomingRefsIndex()

	props := s.Properties()
	out := make([]DependentRelation, 0, 8)

	for _, raw := range props {
		def, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		child, _ := def[constants.SchemaKeyXChildEntity].(map[string]any)
		if child == nil {
			continue
		}

		table, _ := child["table"].(string)
		parentKeyField, _ := child["parent_key_field"].(string)
		if table == "" || parentKeyField == "" {
			continue
		}

		out = append(out, DependentRelation{
			FromTable:     table,
			FromField:     parentKeyField,
			ToKeyField:    s.GetPrimaryKey(),
			IsChildEntity: true,
		})
	}

	incomingRefsIndexMu.RLock()
	incoming := incomingRefsIndex[s.TableName]
	incomingRefsIndexMu.RUnlock()

	if len(incoming) == 0 {
		return out
	}

	seen := make(map[string]struct{}, len(incoming))
	for _, r := range out {
		seen[r.FromTable+"\x00"+r.FromField] = struct{}{}
	}

	for _, r := range incoming {
		k := r.FromTable + "\x00" + r.FromField
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}

	return out
}
