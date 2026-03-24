package schema

import (
	"fmt"
	"sync"
)

var (
	registry = make(map[string]*Schema)
	mu       sync.RWMutex
)

func Register(s *Schema) {
	mu.Lock()
	defer mu.Unlock()
	registry[s.Service+"."+s.Entity] = s
	InvalidateDependentsIndex()
}

func GetByServiceAndEntity(service, entity string) (*Schema, bool) {
	mu.RLock()
	defer mu.RUnlock()
	s, ok := registry[service+"."+entity]
	return s, ok
}

func GetByKey(key string) (*Schema, bool) {
	mu.RLock()
	defer mu.RUnlock()
	s, ok := registry[key]
	return s, ok
}

func GetByTableName(tableName string) (*Schema, bool) {
	mu.RLock()
	defer mu.RUnlock()
	for _, s := range registry {
		if s.TableName == tableName {
			return s, true
		}
	}
	return nil, false
}

func All() []*Schema {
	mu.RLock()
	defer mu.RUnlock()
	schemas := make([]*Schema, 0, len(registry))
	for _, s := range registry {
		schemas = append(schemas, s)
	}
	return schemas
}

func GetByService(service string) []*Schema {
	mu.RLock()
	defer mu.RUnlock()
	schemas := make([]*Schema, 0)
	for _, s := range registry {
		if s.Service == service {
			schemas = append(schemas, s)
		}
	}
	return schemas
}

func TopologicalSorted() ([]*Schema, error) {
	schemas := All()

	byTable := make(map[string]*Schema)
	for _, s := range schemas {
		byTable[s.TableName] = s
	}

	inDegree := make(map[string]int)
	for _, s := range schemas {
		inDegree[s.TableName] = 0
	}
	for _, s := range schemas {
		for _, dep := range s.Dependencies() {
			if _, exists := byTable[dep]; exists {
				inDegree[s.TableName]++
			}
		}
	}

	var queue []*Schema
	for _, s := range schemas {
		if inDegree[s.TableName] == 0 {
			queue = append(queue, s)
		}
	}

	var sorted []*Schema
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		sorted = append(sorted, s)

		for _, other := range schemas {
			for _, dep := range other.Dependencies() {
				if dep == s.TableName {
					inDegree[other.TableName]--
					if inDegree[other.TableName] == 0 {
						queue = append(queue, other)
					}
				}
			}
		}
	}

	if len(sorted) != len(schemas) {
		var stuck []string
		for table, degree := range inDegree {
			if degree > 0 {
				stuck = append(stuck, table)
			}
		}
		return nil, fmt.Errorf("circular dependency detected in schemas: %v", stuck)
	}

	return sorted, nil
}
