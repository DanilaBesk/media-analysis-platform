package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/api"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/queue"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
	"github.com/danila/telegram-transcriber-bot/apps/api/internal/ws"
)

const (
	defaultBindAddr       = "0.0.0.0:8080"
	defaultMaxUploadBytes = 1 << 30
)

type runtimeConfig struct {
	postgresDSN         string
	redisURL            string
	minioEndpoint       string
	minioPublicEndpoint string
	minioAccessKey      string
	minioSecretKey      string
	bindAddr            string
	maxUploadBytes      int64
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalf("[ApiHttp][main][BLOCK_VALIDATE_REQUEST_AND_SHAPE_RESPONSE] startup failed: %v", err)
	}
}

func run(ctx context.Context) error {
	logger := log.New(os.Stdout, "[api] ", log.LstdFlags|log.LUTC)

	cfg, err := loadRuntimeConfig()
	if err != nil {
		return err
	}

	db, err := storage.OpenPostgresDB(ctx, cfg.postgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := ensureSchema(ctx, db); err != nil {
		return err
	}

	stateStore, err := storage.NewSQLStateStore(db)
	if err != nil {
		return err
	}
	minioClient, err := storage.NewMinioClient(cfg.minioEndpoint, cfg.minioAccessKey, cfg.minioSecretKey)
	if err != nil {
		return err
	}
	presignClient := minioClient
	if cfg.minioPublicEndpoint != "" && cfg.minioPublicEndpoint != cfg.minioEndpoint {
		presignClient, err = storage.NewMinioClient(cfg.minioPublicEndpoint, cfg.minioAccessKey, cfg.minioSecretKey)
		if err != nil {
			return err
		}
	}
	objectStore, err := storage.NewMinioObjectStoreWithPresignClient(minioClient, presignClient)
	if err != nil {
		return err
	}
	repository, err := storage.NewRepository(stateStore, objectStore)
	if err != nil {
		return err
	}

	redisOpt, err := parseRedisClientOpt(cfg.redisURL)
	if err != nil {
		return err
	}
	asynqClient := queue.NewAsynqClientAdapter(redisOpt)
	defer asynqClient.Close()

	publisher, err := queue.NewPublisher(asynqClient)
	if err != nil {
		return err
	}
	websocketHub := ws.NewHub()
	eventsService, err := ws.NewService(repository, websocketHub, nil)
	if err != nil {
		return err
	}
	deps, err := api.NewRuntimeDependencies(repository, publisher, eventsService, websocketHub)
	if err != nil {
		return err
	}

	server := api.NewServer(
		deps,
		api.WithLogger(logger),
		api.WithMaxRequestBytes(cfg.maxUploadBytes),
	)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:              cfg.bindAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Printf("listening addr=%s", cfg.bindAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func loadRuntimeConfig() (runtimeConfig, error) {
	cfg := runtimeConfig{
		postgresDSN:         strings.TrimSpace(os.Getenv("POSTGRES_DSN")),
		redisURL:            strings.TrimSpace(os.Getenv("REDIS_URL")),
		minioEndpoint:       strings.TrimSpace(os.Getenv("MINIO_ENDPOINT")),
		minioPublicEndpoint: strings.TrimSpace(os.Getenv("MINIO_PUBLIC_ENDPOINT")),
		minioAccessKey:      strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY")),
		minioSecretKey:      strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY")),
		bindAddr:            strings.TrimSpace(os.Getenv("API_BIND_ADDR")),
		maxUploadBytes:      defaultMaxUploadBytes,
	}
	if cfg.bindAddr == "" {
		cfg.bindAddr = defaultBindAddr
	}
	if cfg.minioPublicEndpoint == "" {
		cfg.minioPublicEndpoint = cfg.minioEndpoint
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "POSTGRES_DSN", value: cfg.postgresDSN},
		{name: "REDIS_URL", value: cfg.redisURL},
		{name: "MINIO_ENDPOINT", value: cfg.minioEndpoint},
		{name: "MINIO_ACCESS_KEY", value: cfg.minioAccessKey},
		{name: "MINIO_SECRET_KEY", value: cfg.minioSecretKey},
	} {
		if field.value == "" {
			return runtimeConfig{}, fmt.Errorf("%s is required", field.name)
		}
	}

	if raw := strings.TrimSpace(os.Getenv("MAX_UPLOAD_SIZE_BYTES")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return runtimeConfig{}, fmt.Errorf("MAX_UPLOAD_SIZE_BYTES must be a positive integer")
		}
		cfg.maxUploadBytes = parsed
	}
	return cfg, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	var relation sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.job_submissions')`).Scan(&relation); err != nil {
		return fmt.Errorf("check schema state: %w", err)
	}
	if relation.Valid {
		return nil
	}

	upMigration, err := loadUpMigration()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, upMigration); err != nil {
		return fmt.Errorf("apply schema bootstrap: %w", err)
	}
	return nil
}

func loadUpMigration() (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve runtime migration path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "internal", "storage", "migrations", "0001_job_control_plane_foundation.sql")
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read migration file: %w", err)
	}
	upSection := strings.Split(string(content), "-- +goose Down")[0]
	upSection = strings.Replace(upSection, "-- +goose Up", "", 1)
	upSection = strings.TrimSpace(upSection)
	if upSection == "" {
		return "", fmt.Errorf("migration up section is empty")
	}
	return upSection, nil
}

func parseRedisClientOpt(raw string) (asynq.RedisClientOpt, error) {
	parsed, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil {
		return asynq.RedisClientOpt{}, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	if parsed.Scheme != "redis" {
		return asynq.RedisClientOpt{}, fmt.Errorf("REDIS_URL must use redis://")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return asynq.RedisClientOpt{}, fmt.Errorf("REDIS_URL host is required")
	}
	port := parsed.Port()
	if port == "" {
		port = "6379"
	}
	dbNumber := 0
	if path := strings.Trim(parsed.Path, "/"); path != "" {
		parsedDB, err := strconv.Atoi(path)
		if err != nil || parsedDB < 0 {
			return asynq.RedisClientOpt{}, fmt.Errorf("REDIS_URL path must be a non-negative DB number")
		}
		dbNumber = parsedDB
	}
	password, _ := parsed.User.Password()
	return asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Username: parsed.User.Username(),
		Password: password,
		DB:       dbNumber,
	}, nil
}
