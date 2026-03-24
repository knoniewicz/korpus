package config

import (
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
)

type Config struct {
	DatabaseURL     string
	RedisAddr       string
	HTTPPort        string
	SchemaDir       string
	MaxWorkers      int
	RedisBufferSize int
	AuthToken       string
}

func LoadConfig() *Config {
	return &Config{
		DatabaseURL:     getDatabaseURL(),
		RedisAddr:       getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		HTTPPort:        getEnv("HTTP_PORT", "4222"),
		SchemaDir:       getEnv("SCHEMA_DIR", "./extensions"),
		MaxWorkers:      getEnvIntAtLeast("MAX_WORKERS", runtime.NumCPU(), 1),
		RedisBufferSize: getEnvIntAtLeast("REDIS_BUFFER_SIZE", 10000, 1),
		AuthToken:       getEnv("AUTH_TOKEN", ""),
	}
}

func getDatabaseURL() string {
	sslMode := getEnv("POSTGRES_SSLMODE", "disable")

	// First try DATABASE_URL
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		parsedURL, err := url.Parse(dbURL)
		if err != nil {
			// Preserve prior behavior if an invalid DATABASE_URL is supplied.
			return dbURL
		}

		query := parsedURL.Query()
		if query.Get("sslmode") == "" && sslMode != "" {
			query.Set("sslmode", sslMode)
			parsedURL.RawQuery = query.Encode()
			return parsedURL.String()
		}

		return dbURL
	}

	// Fall back to individual PostgreSQL environment variables
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	db := os.Getenv("POSTGRES_DB")

	if user == "" || password == "" || host == "" || port == "" || db == "" {
		return ""
	}

	connectionURL := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + db,
	}

	query := connectionURL.Query()
	if sslMode != "" {
		query.Set("sslmode", sslMode)
	}
	connectionURL.RawQuery = query.Encode()

	return connectionURL.String()
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvIntAtLeast(key string, fallback, min int) int {
	value := getEnvInt(key, fallback)
	if value < min {
		if fallback >= min {
			return fallback
		}
		return min
	}
	return value
}
