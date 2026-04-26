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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/danila/media-analysis-platform/apps/api/internal/api"
	"github.com/danila/media-analysis-platform/apps/api/internal/queue"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
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
	webhookDelivery     bool
	webhookInterval     time.Duration
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
	var webhookDispatcher ws.Dispatcher
	if cfg.webhookDelivery {
		dispatcher, err := ws.NewHTTPWebhookDispatcher(repository, ws.WithWebhookLogger(logger))
		if err != nil {
			return err
		}
		webhookDispatcher = dispatcher
		go dispatcher.Run(ctx, cfg.webhookInterval, 20)
	}
	eventsService, err := ws.NewService(repository, websocketHub, webhookDispatcher)
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
		webhookInterval:     time.Second,
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
	if raw := strings.TrimSpace(os.Getenv("WEBHOOK_DELIVERY_ENABLED")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return runtimeConfig{}, fmt.Errorf("WEBHOOK_DELIVERY_ENABLED must be boolean")
		}
		cfg.webhookDelivery = enabled
	}
	if raw := strings.TrimSpace(os.Getenv("WEBHOOK_DELIVERY_INTERVAL_SECONDS")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed <= 0 {
			return runtimeConfig{}, fmt.Errorf("WEBHOOK_DELIVERY_INTERVAL_SECONDS must be positive")
		}
		cfg.webhookInterval = time.Duration(parsed * float64(time.Second))
	}
	return cfg, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    name text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)
`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	applied, err := loadAppliedMigrations(ctx, db)
	if err != nil {
		return err
	}
	if len(migrations) > 0 {
		hasBootstrapSchema, err := schemaRelationExists(ctx, db, "public.job_submissions")
		if err != nil {
			return err
		}
		if hasBootstrapSchema {
			baseline := migrations[0].Name
			if _, ok := applied[baseline]; !ok {
				if err := recordAppliedMigration(ctx, db, baseline); err != nil {
					return err
				}
				applied[baseline] = struct{}{}
			}
		}
	}
	for _, migration := range migrations {
		if _, ok := applied[migration.Name]; ok {
			continue
		}
		if err := applyMigration(ctx, db, migration); err != nil {
			return err
		}
	}
	return nil
}

type migrationSpec struct {
	Name string
	Up   string
	Down string
}

func loadMigrations() ([]migrationSpec, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("resolve runtime migration path")
	}
	pattern := filepath.Join(filepath.Dir(currentFile), "..", "..", "internal", "storage", "migrations", "*.sql")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob migration files: %w", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no migration files found")
	}
	migrations := make([]migrationSpec, 0, len(paths))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration file %s: %w", path, err)
		}
		upSection, downSection, err := parseMigrationSections(string(content))
		if err != nil {
			return nil, fmt.Errorf("parse migration %s: %w", path, err)
		}
		migrations = append(migrations, migrationSpec{
			Name: filepath.Base(path),
			Up:   upSection,
			Down: downSection,
		})
	}
	return migrations, nil
}

func parseMigrationSections(content string) (string, string, error) {
	parts := strings.Split(content, "-- +goose Down")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("migration must contain exactly one goose Down marker")
	}
	upSection := strings.TrimSpace(strings.Replace(parts[0], "-- +goose Up", "", 1))
	downSection := strings.TrimSpace(parts[1])
	if upSection == "" || downSection == "" {
		return "", "", fmt.Errorf("migration up/down sections must both be non-empty")
	}
	return upSection, downSection, nil
}

func loadAppliedMigrations(ctx context.Context, db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("list applied migrations: %w", err)
	}
	defer rows.Close()

	applied := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func schemaRelationExists(ctx context.Context, db *sql.DB, relationName string) (bool, error) {
	var relation sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)`, relationName).Scan(&relation); err != nil {
		return false, fmt.Errorf("check schema state: %w", err)
	}
	return relation.Valid, nil
}

func recordAppliedMigration(ctx context.Context, db *sql.DB, migrationName string) error {
	if _, err := db.ExecContext(ctx, `
INSERT INTO schema_migrations (name)
VALUES ($1)
ON CONFLICT (name) DO NOTHING
`, migrationName); err != nil {
		return fmt.Errorf("record baseline migration %s: %w", migrationName, err)
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, migration migrationSpec) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.Name, err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
		return fmt.Errorf("apply migration %s: %w", migration.Name, err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO schema_migrations (name)
VALUES ($1)
ON CONFLICT (name) DO NOTHING
`, migration.Name); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.Name, err)
	}
	tx = nil
	return nil
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
