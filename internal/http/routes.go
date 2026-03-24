package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"golang.org/x/time/rate"

	"github.com/knoniewicz/korpus/internal/config"
	"github.com/knoniewicz/korpus/internal/constants"
	"github.com/knoniewicz/korpus/internal/database"
	"github.com/knoniewicz/korpus/internal/events"
	"github.com/knoniewicz/korpus/internal/redisc"
	"github.com/knoniewicz/korpus/internal/schema"
)

var sonicJSON = sonic.ConfigFastest

type Router struct {
	handler *events.Handler
	db      *database.DB
	redis   *redisc.Client
	qdrant  *qdrant.Client
	limiter *rate.Limiter
	cfg     *config.Config
}

func NewRouter(handler *events.Handler, db *database.DB, redis *redisc.Client, cfg *config.Config) *Router {
	return &Router{
		handler: handler,
		db:      db,
		redis:   redis,
		limiter: rate.NewLimiter(rate.Limit(10), 100),
		cfg:     cfg,
	}
}

func respondJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(code)
	sonicJSON.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, r *http.Request, code int, message string) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(code)

	requestID, ok := r.Context().Value(constants.CtxKeyRequestID).(string)
	if !ok {
		requestID = constants.StatusUnknown
	}
	response := map[string]string{
		"error":   message,
		"details": fmt.Sprintf("see logs for request-id %s", requestID),
	}

	sonicJSON.NewEncoder(w).Encode(response)
}

func (router *Router) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if router.cfg.AuthToken == "" {
			respondError(w, r, http.StatusUnauthorized, "authentication not configured")
			return
		}
		token := r.Header.Get(constants.HeaderKorpusToken)
		if token != router.cfg.AuthToken {
			respondError(w, r, http.StatusUnauthorized, constants.ErrUnauthorized)
			return
		}
		next(w, r)
	}
}

func (router *Router) rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !router.limiter.Allow() {
			respondError(w, r, http.StatusTooManyRequests, constants.ErrRateLimitExceeded)
			return
		}
		next(w, r)
	}
}

func (router *Router) recover(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				stack := debug.Stack()
				slog.Error("panic recovered in http handler",
					"error", err,
					"path", r.URL.Path,
					"method", r.Method,
					"stack", string(stack))

				respondError(w, r, http.StatusInternalServerError, constants.ErrInternalServerError)
			}
		}()
		next(w, r)
	}
}

func Chain(handler http.HandlerFunc, middlewares ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

func (router *Router) RegisterRoutes(mux *http.ServeMux) {
	// data routes (get entities, get schema, resolve lookups, get vectors)
	mux.Handle("GET /entities", Chain(router.getEntities, router.recover, router.authenticate, router.rateLimit))
	mux.Handle("GET /schemas", Chain(router.getSchemas, router.recover, router.authenticate, router.rateLimit))

	// information routes (what schemas are available, what events are being sent)
	mux.Handle("GET /events", Chain(router.eventSenderEndpoint, router.recover, router.authenticate, router.rateLimit))

	// event write route (frontend submits create/update/delete intents)
	mux.Handle("POST /events/publish", Chain(router.publishEvent, router.recover, router.authenticate, router.rateLimit))

	// health and metrics routes
	mux.Handle("GET /health", Chain(router.health, router.recover, router.rateLimit))
}

func (router *Router) getEntity(r *http.Request, entitySchema *schema.Schema) ([]map[string]interface{}, error) {
	includeChildren := r.URL.Query().Get("include_children") != "false"

	qb := BuildQuery(r, entitySchema)
	resolves := r.URL.Query().Get("resolve")

	if resolves != "" {
		qb.ResolveAll(includeChildren)
		if depthStr := r.URL.Query().Get("depth"); depthStr != "" {
			if depth, err := strconv.Atoi(depthStr); err == nil && depth > 0 {
				qb.Depth(depth)
			}
		}
	}

	entities, err := qb.Exec(r.Context(), router.db.Conn)
	if err != nil {
		return nil, fmt.Errorf(constants.ErrFailedToGetEntities)
	}

	return entities, nil
}

func (router *Router) getEntities(w http.ResponseWriter, r *http.Request) {
	service, entity := parseEntityPath(r)
	if service != "" && entity != "" {
		entitySchema, ok := schema.GetByServiceAndEntity(service, entity)
		if !ok {
			respondError(w, r, http.StatusNotFound, constants.ErrFailedToGetSchema)
			return
		}
		entities, err := router.getEntity(r, entitySchema)
		if err != nil {
			respondError(w, r, http.StatusInternalServerError, constants.ErrFailedToGetEntities)
			return
		}

		// Ensure we return an array, not null
		if entities == nil {
			entities = []map[string]interface{}{}
		}

		respondJSON(w, http.StatusOK, entities)
		return
	}

	schemas := schema.All()
	if service != "" {
		schemas = schema.GetByService(service)
	}

	result := make([]map[string]interface{}, 0, len(schemas))
	for _, s := range schemas {
		entities, err := router.getEntity(r, s)
		if err != nil {
			slog.Error("failed to get entities", "service", s.Service, "entity", s.Entity, "error", err)
			continue
		}
		if len(entities) == 0 {
			continue
		}
		result = append(result, map[string]interface{}{
			"service":  s.Service,
			"entity":   s.Entity,
			"entities": entities,
		})
	}
	respondJSON(w, http.StatusOK, result)
}

func (router *Router) eventSenderEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	slog.Info("server event channel", "channel", router.handler.ServerEventChannel)
	flusher, ok := w.(http.Flusher)
	if !ok {
		respondError(w, r, http.StatusInternalServerError, constants.ErrInternalServerError)
		return
	}

	for eventJSON := range router.handler.ServerEventChannel {
		select {
		case <-r.Context().Done():
			return
		default:
			if _, err := fmt.Fprintln(w, eventJSON); err != nil {
				slog.Error("sse write error", "error", err)
				return
			}
			flusher.Flush()
		}
	}
}

func (router *Router) getSchemas(w http.ResponseWriter, r *http.Request) {
	service, entity := parseEntityPath(r)

	if service != "" && entity != "" {
		s, ok := schema.GetByServiceAndEntity(service, entity)
		if !ok {
			slog.Error("failed to get schema", "service", service, "entity", entity)
			respondError(w, r, http.StatusNotFound, constants.ErrFailedToGetSchema)
			return
		}
		description, _ := s.Data["description"].(string)
		schemaInformation := map[string]interface{}{
			"service":      s.Service,
			"entity":       s.Entity,
			"description":  description,
			"schema":       s.Data,
			"dependencies": s.Dependencies(),
			"dependents":   s.Dependents(),
		}
		respondJSON(w, http.StatusOK, schemaInformation)
		return
	}

	schemas := schema.All()
	if service != "" {
		schemas = schema.GetByService(service)
	}

	result := make([]map[string]interface{}, 0, len(schemas))
	for _, s := range schemas {
		description, _ := s.Data["description"].(string)

		result = append(result, map[string]interface{}{
			"service":      s.Service,
			"entity":       s.Entity,
			"description":  description,
			"dependencies": s.Dependencies(),
			"dependents":   s.Dependents(),
		})
	}
	respondJSON(w, http.StatusOK, result)
}

func (router *Router) health(w http.ResponseWriter, r *http.Request) {
	status := map[string]string{
		constants.HealthStatusStatus: constants.HealthStatusOK,
	}

	if err := router.db.Ping(); err != nil {
		status[constants.HealthStatusDatabase] = constants.HealthStatusUnhealthy
		status[constants.HealthStatusStatus] = constants.HealthStatusDegraded
	} else {
		status[constants.HealthStatusDatabase] = constants.HealthStatusHealthy
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := router.redis.Ping(ctx); err != nil {
		status[constants.HealthStatusRedis] = constants.HealthStatusUnhealthy
		status[constants.HealthStatusStatus] = constants.HealthStatusDegraded
	} else {
		status[constants.HealthStatusRedis] = constants.HealthStatusHealthy
	}

	code := http.StatusOK
	if status[constants.HealthStatusStatus] != constants.HealthStatusOK {
		code = http.StatusServiceUnavailable
	}

	respondJSON(w, code, status)
}

func (router *Router) publishEvent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Service string                 `json:"service"`
		Entity  string                 `json:"entity"`
		Action  string                 `json:"action"`
		Payload map[string]interface{} `json:"payload"`
	}

	if err := sonicJSON.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, r, http.StatusBadRequest, constants.ErrInvalidBody)
		return
	}

	if req.Service == "" || req.Entity == "" || req.Action == "" || req.Payload == nil {
		respondError(w, r, http.StatusBadRequest, "missing required field: service, entity, action, or payload")
		return
	}

	validActions := map[string]bool{
		constants.ActionCreated: true,
		constants.ActionEnded:   true,
		constants.ActionDeleted: true,
	}

	if !validActions[req.Action] {
		respondError(w, r, http.StatusBadRequest, "invalid action: must be 'created', 'ended', or 'deleted'")
		return
	}

	entitySchema, ok := schema.GetByServiceAndEntity(req.Service, req.Entity)
	if !ok {
		respondError(w, r, http.StatusBadRequest, "unknown service or entity")
		return
	}

	if err := entitySchema.Validate(nil, req.Payload); err != nil {
		respondError(w, r, http.StatusBadRequest, fmt.Sprintf("schema validation failed: %v", err))
		return
	}

	channel := fmt.Sprintf("%s.%s.%s", req.Service, req.Entity, req.Action)

	envelope, err := router.redis.ToRedisJSON(r.Context(), req.Payload)
	if err != nil {
		slog.Error("failed to create redis envelope", "error", err)
		respondError(w, r, http.StatusInternalServerError, constants.ErrInternalServerError)
		return
	}

	if err := router.redis.Publish(r.Context(), channel, envelope); err != nil {
		slog.Error("failed to publish event", "channel", channel, "error", err)
		respondError(w, r, http.StatusInternalServerError, constants.ErrInternalServerError)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  constants.StatusSuccess,
		"channel": channel,
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, x-korpus-token")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(constants.HeaderXRequestID)
		if requestID == "" {
			requestID = uuid.New().String()
		}

		ctx := context.WithValue(r.Context(), constants.CtxKeyRequestID, requestID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func middleware(next http.Handler) http.Handler {
	return loggingMiddleware(corsMiddleware(next))
}

func (router *Router) NewServer() *http.Server {
	mux := http.NewServeMux()
	router.RegisterRoutes(mux)
	return &http.Server{
		Addr:    fmt.Sprintf(":%s", router.cfg.HTTPPort),
		Handler: middleware(mux),
	}
}
