package main

import (
	"context"
	"log/slog"
	net_http "net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/config"
	"github.com/knoniewicz/korpus/internal/database"
	"github.com/knoniewicz/korpus/internal/env_loader"
	"github.com/knoniewicz/korpus/internal/events"
	"github.com/knoniewicz/korpus/internal/http"
	"github.com/knoniewicz/korpus/internal/redisc"
	"github.com/knoniewicz/korpus/internal/service"

	_ "net/http/pprof" // Add this import
)

type App struct {
	db      *database.DB
	redis   *redisc.Client
	handler *events.Handler
	router  *http.Router
	cfg     *config.Config
}

func New(ctx context.Context, cfg *config.Config) (*App, error) {
	writeCh := make(chan *channel.Event, 10000)
	db, err := database.New(cfg, writeCh)
	if err != nil {
		return nil, err
	}

	if err := db.CreateTables(ctx); err != nil {
		return nil, err
	}

	redisClient, err := redisc.New(cfg.RedisAddr)
	if err != nil {
		return nil, err
	}

	sessionService := service.NewSessionService(db, redisClient)
	handler := events.NewHandler(ctx, sessionService, cfg.MaxWorkers, writeCh)
	router := http.NewRouter(handler, db, redisClient, cfg)

	return &App{
		db:      db,
		redis:   redisClient,
		handler: handler,
		router:  router,
		cfg:     cfg,
	}, nil
}

func (app *App) Listen(ctx context.Context, pattern string) {
	sub := app.redis.Subscribe(ctx, pattern)
	defer sub.Close()

	slog.Info("redis listener started", "pattern", pattern)
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			slog.Info("redis listener stopped", "reason", "context cancelled")
			return
		case msg, ok := <-ch:
			if !ok {
				slog.Warn("redis listener stopped", "reason", "channel closed")
				return
			}
			app.handler.HandleRedisMessage(msg)
		}
	}
}

func (app *App) Serve(ctx context.Context) {
	server := app.router.NewServer()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("http server shutdown error", "error", err)
		}
	}()

	go func() {
		slog.Info("http server started", "port", app.cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil {
			slog.Error("http server error", "error", err)
		}
	}()
}

func (app *App) Close() {
	if err := app.db.Close(); err != nil {
		slog.Error("failed to close database", "error", err)
	}
	if err := app.redis.Close(); err != nil {
		slog.Error("failed to close redis", "error", err)
	}
}

func safeRedisAddr(addr string) string {
	if addr == "" {
		return ""
	}

	parsed, err := url.Parse(addr)
	if err == nil && parsed.Scheme != "" {
		parsed.User = nil
		return parsed.String()
	}

	if strings.Contains(addr, "@") {
		return "[redacted]"
	}

	return addr
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	if err := env_loader.Load(); err != nil {
		slog.Error("failed to load environment", "error", err)
		os.Exit(1)
	}

	cfg := config.LoadConfig()
	slog.Info(
		"korpus starting",
		"http_port", cfg.HTTPPort,
		"schema_dir", cfg.SchemaDir,
		"max_workers", cfg.MaxWorkers,
		"redis_addr", safeRedisAddr(cfg.RedisAddr),
		"redis_buffer_size", cfg.RedisBufferSize,
		"database_configured", cfg.DatabaseURL != "",
		"auth_token_configured", cfg.AuthToken != "",
	)

	go func() {
		slog.Info("pprof server started", "port", "6060")
		if err := net_http.ListenAndServe("localhost:6060", nil); err != nil {
			slog.Error("pprof server failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := New(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize app", "error", err)
		os.Exit(1)
	}
	defer app.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		app.Listen(ctx, "*")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		app.Serve(ctx)
	}()

	<-ctx.Done()
	slog.Info("shutdown initiated")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("graceful shutdown complete")
	case <-time.After(10 * time.Second):
		slog.Warn("shutdown timeout, forcing exit")
	}
}
