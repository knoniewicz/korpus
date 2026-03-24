package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/knoniewicz/korpus/internal/channel"
	"github.com/knoniewicz/korpus/internal/config"

	_ "github.com/lib/pq"
)

type DB struct {
	Conn    *sql.DB
	writeCh chan *channel.Event
	config  *config.Config
}

func New(cfg *config.Config, writeCh chan *channel.Event) (*DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("database is not configured: set DATABASE_URL or POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_HOST/POSTGRES_PORT/POSTGRES_DB")
	}

	conn, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	if err = conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)

	return &DB{
		Conn:    conn,
		writeCh: writeCh,
		config:  cfg,
	}, nil
}

func (db *DB) recordOperation(operation string, status string, duration time.Duration) {
	_ = operation
	_ = status
	_ = duration
}

func (db *DB) Close() error {
	return db.Conn.Close()
}

func (db *DB) Ping() error {
	return db.Conn.Ping()
}
