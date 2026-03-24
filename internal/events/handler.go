package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"

	"github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/constants"
	"github.com/knoniewicz/korpus/internal/schema"
	"github.com/knoniewicz/korpus/internal/service"
)

type Handler struct {
	svc                service.SessionService
	workers            int
	jobsCh             chan *redis.Message
	writeCh            chan *channel.Event
	workersWg          sync.WaitGroup
	writersWg          sync.WaitGroup
	retriesWg          sync.WaitGroup
	writeProducersWg   sync.WaitGroup
	closed             bool
	mu                 sync.Mutex
	ServerEventChannel chan string
	retrySem           chan struct{}
}

const (
	foreignKeyRetryDelay = 1000 * time.Millisecond // 1 second timer between retries
	maxRetryAttempts     = 5
	jobsEnqueueTimeout   = 2 * time.Second
)

func NewHandler(ctx context.Context, svc service.SessionService, workers int, writeCh chan *channel.Event) *Handler {
	h := &Handler{
		svc:                svc,
		workers:            workers,
		jobsCh:             make(chan *redis.Message, 10000),
		writeCh:            writeCh,
		ServerEventChannel: make(chan string, 1000),
		retrySem:           make(chan struct{}, 100),
	}

	go h.updateQueueMetrics(ctx)
	for range workers {
		h.workersWg.Add(1)
		go h.workerLoop(ctx)
	}

	for range workers {
		h.writersWg.Add(1)
		go h.dbWriterLoop(ctx)
	}

	go func() {
		<-ctx.Done()
		h.Close()
	}()
	return h
}

func (h *Handler) workerLoop(ctx context.Context) {
	defer h.workersWg.Done()
	for msg := range h.jobsCh {
		h.Process(ctx, msg)
	}
}

func (handler *Handler) Process(ctx context.Context, msg *redis.Message) {
	if strings.HasPrefix(msg.Channel, constants.ChannelPrefixKorpus) {
		return
	}

	event, err := handler.verifySchema(msg)
	if err != nil {
		slog.Error("schema verification failed", "error", err, "channel", msg.Channel)
		return
	}

	if !handler.enqueueWriteEvent(event) {
		event.Release()
	}

}

func (handler *Handler) verifySchema(msg *redis.Message) (*channel.Event, error) {
	event, err := channel.NewEvent(msg)
	if err != nil {
		return nil, fmt.Errorf("invalid channel format: %v", err)
	}

	schema, ok := schema.GetByKey(event.Key)
	if !ok {
		event.Release()
		return nil, fmt.Errorf("schema not found: %s", event.Key)
	}

	if err := schema.Validate(event, event.Payload); err != nil {
		event.Release()
		return nil, fmt.Errorf("validation failed: %v", err)
	}

	return event, nil
}

func (h *Handler) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	h.mu.Unlock()

	slog.Info("handler shutdown initiated, draining channels")

	// Stop worker intake first, then wait for all workers to finish producing DB write events.
	close(h.jobsCh)
	h.workersWg.Wait()
	h.retriesWg.Wait()
	h.writeProducersWg.Wait()

	// Once workers are done, no more regular writers can enqueue, so db writers can drain and exit.
	close(h.writeCh)
	h.writersWg.Wait()

	slog.Info("handler shutdown complete")
}

func (h *Handler) HandleRedisMessage(msg *redis.Message) {
	// We do a bounded blocking enqueue to apply backpressure when the worker queue is full.
	// This intentionally avoids the previous non-blocking "drop" behavior that could lose
	// events silently under load.
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("failed to enqueue redis message during shutdown", "channel", msg.Channel, "panic", r)
		}
	}()

	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		slog.Warn("handler already closed; rejecting redis message", "channel", msg.Channel)
		return
	}

	timer := time.NewTimer(jobsEnqueueTimeout)
	defer timer.Stop()

	select {
	case h.jobsCh <- msg:
		return
	case <-timer.C:
		// Explicit error path: we could not enqueue in time, so surface this condition clearly
		// instead of dropping without visibility.
		slog.Error(
			"timed out enqueueing redis message; queue saturated",
			"channel", msg.Channel,
			"timeout", jobsEnqueueTimeout,
			"queue_len", len(h.jobsCh),
			"queue_cap", cap(h.jobsCh),
		)
	}
}

func (h *Handler) dbWriterLoop(ctx context.Context) {
	defer h.writersWg.Done()
	for event := range h.writeCh {
		h.processEvent(ctx, event)
	}
}

func (h *Handler) processEvent(ctx context.Context, event *channel.Event) {
	var err error

	switch event.Action {
	case constants.ActionCreated:
		err = h.svc.Create(ctx, event)
	case constants.ActionEnded:
		err = h.svc.End(ctx, event)
	case constants.ActionDeleted:
		err = h.svc.Delete(ctx, event)
	default:
		err = fmt.Errorf("unknown action: %s", event.Action)
	}

	jsonStr, marshalErr := sonic.MarshalString(event)
	if marshalErr != nil {
		slog.Error("failed to marshal event", "error", marshalErr)
	} else {
		select {
		case h.ServerEventChannel <- jsonStr:
		default:
			slog.Debug("ServerEventChannel full, dropping event", "request_id", event.RequestID)
		}
	}

	if err != nil {
		if errors.Is(err, constants.ErrRetryableInsert) && event.Action == constants.ActionCreated {
			h.scheduleRetry(ctx, event, err)
			return
		}

		slog.Error("event processing failed", "error", err, "channel", event.Key, "action", event.Action, "request_id", event.RequestID)
		event.Release()
		return
	}

	event.Release()
}

func (h *Handler) updateQueueMetrics(ctx context.Context) {
	<-ctx.Done()
}

func (h *Handler) beginWriteProduce() bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return false
	}

	h.writeProducersWg.Add(1)
	return true
}

func (h *Handler) enqueueWriteEvent(event *channel.Event) bool {
	if !h.beginWriteProduce() {
		return false
	}

	defer h.writeProducersWg.Done()
	h.writeCh <- event
	return true
}

func (h *Handler) scheduleRetry(ctx context.Context, event *channel.Event, err error) {
	attempt := event.IncrementRetry()
	if attempt > maxRetryAttempts {
		slog.Error("max retries exceeded for event", "channel", event.Key, "request_id", event.RequestID, "attempts", attempt, "error", err)
		event.Release()
		return
	}

	slog.Warn("retrying event due to dependency constraint", "channel", event.Key, "request_id", event.RequestID, "attempt", attempt, "error", err)

	h.mu.Lock()
	if h.closed || ctx.Err() != nil {
		h.mu.Unlock()
		event.Release()
		return
	}
	h.retriesWg.Add(1)
	h.mu.Unlock()

	go func(ev *channel.Event) {
		defer h.retriesWg.Done()

		select {
		case h.retrySem <- struct{}{}:
			defer func() {
				<-h.retrySem
			}()
		case <-ctx.Done():
			ev.Release()
			return
		}

		select {
		case <-ctx.Done():
			ev.Release()
			return
		case <-time.After(foreignKeyRetryDelay):
			if ctx.Err() != nil {
				ev.Release()
				return
			}
			if !h.enqueueWriteEvent(ev) {
				ev.Release()
			}
		}
	}(event)
}
