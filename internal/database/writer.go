package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/constants"
	"github.com/knoniewicz/korpus/internal/schema"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

func (db *DB) InsertEvent(ctx context.Context, event *channel.Event, schema *schema.Schema) (string, error) {
	tableSchema := schema.GetTableSchema()
	primaryKey, children, err := tableSchema.Upsert(ctx, event.Payload, event.RequestID)
	if err != nil {
		if isForeignKeyViolation(err) {
			return "", fmt.Errorf("%w: %v", constants.ErrRetryableInsert, err)
		}
		return "", err
	}

	for _, child := range children {
		slog.Debug("processing child entity", "child_key", child.Key, "parent_key", child.ParentKey)
		db.writeCh <- &channel.Event{
			RequestID: uuid.New().String(),
			Action:    constants.ActionCreated,
			Key:       child.Key,
			Payload:   child.Payload,
		}
	}

	// return the primary key
	return primaryKey, nil
}

func (db *DB) EndEvent(ctx context.Context, event *channel.Event) error {
	endedAt, ok := event.Payload[constants.FieldEndedAt]
	if !ok {
		slog.Warn("ended_at not found, setting to current time", "request_id", event.RequestID)
		endedAt = time.Now().UTC().Format(time.RFC3339)
	} else {
		if endedAtStr, ok := endedAt.(string); ok {
			endedAt = endedAtStr
		} else {
			endedAt = time.Now().UTC().Format(time.RFC3339)
		}
	}

	tableSchema, ok := schema.GetTableSchemaByKey(event.Key)
	if !ok {
		return fmt.Errorf("event update not found: %s", event.Key)
	}

	primaryKeyCol := tableSchema.PrimaryKeyCol
	primaryKey, exists := event.Payload[primaryKeyCol]
	if !exists {
		return fmt.Errorf("primary key '%s' required for end event", primaryKeyCol)
	}

	primaryKeyStr, ok := primaryKey.(string)
	if !ok {
		return fmt.Errorf("primary key is not a string")
	}

	return tableSchema.EndSession(ctx, primaryKeyStr, endedAt.(string))
}

func (db *DB) DeleteEvent(ctx context.Context, event *channel.Event, primaryKeyCol string) error {
	tableSchema, ok := schema.GetTableSchemaByKey(event.Key)
	if !ok {
		return fmt.Errorf("event delete not found: %s", event.Key)
	}

	primaryKeyValue, ok := event.Payload[primaryKeyCol]
	if !ok {
		return fmt.Errorf("primary key column '%s' not found in event payload", primaryKeyCol)
	}

	return tableSchema.SoftDelete(ctx, primaryKeyValue)
}

func isForeignKeyViolation(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23503" { // foreign_key_violation
		return true
	}
	return false
}
