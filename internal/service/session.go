package service

import (
	"context"
	"fmt"
	"log/slog"

	ch "github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/constants"
	"github.com/knoniewicz/korpus/internal/database"
	"github.com/knoniewicz/korpus/internal/redisc"
	"github.com/knoniewicz/korpus/internal/schema"

	"github.com/bytedance/sonic"
)

var sonicJSON = sonic.ConfigFastest

type SessionService interface {
	Create(ctx context.Context, event *ch.Event) error
	End(ctx context.Context, event *ch.Event) error
	Delete(ctx context.Context, event *ch.Event) error
}

type sessionService struct {
	db    *database.DB
	redis *redisc.Client
}

func NewSessionService(db *database.DB, redis *redisc.Client) SessionService {
	return &sessionService{
		db:    db,
		redis: redis,
	}
}

func (s *sessionService) publishNotification(ctx context.Context, action string, event *ch.Event) error {

	envelope := ch.Envelope{RequestID: event.RequestID, Payload: event.Payload}
	envelopeJSON, _ := sonicJSON.MarshalToString(envelope)
	err := s.redis.Publish(ctx, constants.ChannelPrefixKorpus+event.Key+"."+action, envelopeJSON)

	return err
}

func (s *sessionService) Create(ctx context.Context, event *ch.Event) error {
	schema, ok := schema.GetByKey(event.Key)
	if !ok {
		return fmt.Errorf("event insert not found: %s", event.Key)
	}

	primaryKey, err := s.db.InsertEvent(ctx, event, schema)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}
	event.Payload[schema.GetTableSchema().PrimaryKeyCol] = primaryKey

	if err := s.publishNotification(ctx, constants.ActionCreated, event); err != nil {
		slog.Error("failed to publish notification", "error", err)
	}

	return nil
}

func (s *sessionService) End(ctx context.Context, event *ch.Event) error {
	schema, ok := schema.GetByKey(event.Key)

	if !ok {
		return fmt.Errorf("event insert not found: %s", event.Key)
	}

	// fast track to return so we don't waste time
	if !schema.GetTableSchema().IsSessionScoped {
		return fmt.Errorf("event is not session scoped")
	}

	if err := s.db.EndEvent(ctx, event); err != nil {
		return err
	}

	if err := s.publishNotification(ctx, constants.ActionEnded, event); err != nil {
		slog.Error("failed to publish notification", "error", err, "channel", constants.ChannelPrefixKorpus+event.Key+"."+constants.ActionEnded, "request_id", event.RequestID)
	}

	return nil
}

func (s *sessionService) Delete(ctx context.Context, event *ch.Event) error {
	schema, ok := schema.GetByKey(event.Key)
	if !ok {
		return fmt.Errorf("event insert not found: %s", event.Key)
	}

	primaryKeyCol := schema.GetTableSchema().PrimaryKeyCol

	if _, exists := event.Payload[primaryKeyCol]; !exists {
		return nil
	}

	if err := s.db.DeleteEvent(ctx, event, primaryKeyCol); err != nil {
		return err
	}

	if err := s.publishNotification(ctx, constants.ActionDeleted, event); err != nil {
		slog.Error("failed to publish notification", "error", err, "channel", constants.ChannelPrefixKorpus+event.Key+"."+constants.ActionDeleted, "request_id", event.RequestID)
	}

	return nil
}
