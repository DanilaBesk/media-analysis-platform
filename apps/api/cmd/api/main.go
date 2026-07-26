package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/api"
	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
	"github.com/danila/media-analysis-platform/apps/api/internal/ws"
)

const (
	defaultBindAddr          = "127.0.0.1:8080"
	defaultMaxUploadBytes    = 1 << 30
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 15 * time.Minute
	defaultWriteTimeout      = 2 * time.Minute
	defaultIdleTimeout       = 2 * time.Minute
	defaultMaxHeaderBytes    = 1 << 20
)

type runtimeConfig struct {
	postgresDSN               string
	minioEndpoint             string
	minioPublicEndpoint       string
	minioAccessKey            string
	minioSecretKey            string
	bindAddr                  string
	maxUploadBytes            int64
	internalToken             string
	mediaRetentionDays        int
	retentionSweepInterval    time.Duration
	retentionBatchSize        int
	retentionClaimDuration    time.Duration
	objectOrphanGrace         time.Duration
	exportDeliveryTTL         time.Duration
	exportWebAccessTTL        time.Duration
	exportDeliveryMaxAttempts int
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
	targetStateStore, err := targetstore.NewStore(db)
	if err != nil {
		return err
	}

	websocketHub := ws.NewHub()
	deps, err := api.NewRuntimeDependenciesWithTargetObjectStore(
		targetStateStore,
		objectStore,
		websocketHub,
		api.WithTargetMediaLifecycle(
			cfg.mediaRetentionDays,
			cfg.exportDeliveryTTL,
			cfg.exportWebAccessTTL,
			cfg.exportDeliveryMaxAttempts,
		),
		api.WithTargetObjectOrphanGrace(cfg.objectOrphanGrace),
	)
	if err != nil {
		return err
	}

	server := api.NewServer(
		deps,
		api.WithLogger(logger),
		api.WithMaxRequestBytes(cfg.maxUploadBytes),
		api.WithStrictLocalRequests(true),
		api.WithInternalToken(cfg.internalToken),
	)
	go runRetentionLoop(ctx, deps.Target, cfg, logger)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:              cfg.bindAddr,
		Handler:           mux,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    defaultMaxHeaderBytes,
	}

	logger.Printf("listening addr=%s", cfg.bindAddr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func loadRuntimeConfig() (runtimeConfig, error) {
	cfg := runtimeConfig{
		postgresDSN:               strings.TrimSpace(os.Getenv("POSTGRES_DSN")),
		minioEndpoint:             strings.TrimSpace(os.Getenv("MINIO_ENDPOINT")),
		minioPublicEndpoint:       strings.TrimSpace(os.Getenv("MINIO_PUBLIC_ENDPOINT")),
		minioAccessKey:            strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY")),
		minioSecretKey:            strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY")),
		bindAddr:                  strings.TrimSpace(os.Getenv("API_BIND_ADDR")),
		maxUploadBytes:            defaultMaxUploadBytes,
		internalToken:             strings.TrimSpace(os.Getenv("PLATFORM_INTERNAL_TOKEN")),
		mediaRetentionDays:        7,
		retentionSweepInterval:    5 * time.Minute,
		retentionBatchSize:        100,
		retentionClaimDuration:    2 * time.Minute,
		objectOrphanGrace:         time.Hour,
		exportDeliveryTTL:         24 * time.Hour,
		exportWebAccessTTL:        24 * time.Hour,
		exportDeliveryMaxAttempts: 5,
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
		{name: "MINIO_ENDPOINT", value: cfg.minioEndpoint},
		{name: "MINIO_ACCESS_KEY", value: cfg.minioAccessKey},
		{name: "MINIO_SECRET_KEY", value: cfg.minioSecretKey},
		{name: "PLATFORM_INTERNAL_TOKEN", value: cfg.internalToken},
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
	var parseErr error
	if cfg.mediaRetentionDays, parseErr = positiveIntEnv("MEDIA_OBJECT_RETENTION_DAYS", cfg.mediaRetentionDays); parseErr != nil {
		return runtimeConfig{}, parseErr
	}
	if sweepSeconds, err := positiveIntEnv("RETENTION_SWEEP_INTERVAL_SECONDS", int(cfg.retentionSweepInterval/time.Second)); err != nil {
		return runtimeConfig{}, err
	} else {
		cfg.retentionSweepInterval = time.Duration(sweepSeconds) * time.Second
	}
	if cfg.retentionBatchSize, parseErr = positiveIntEnv("RETENTION_BATCH_SIZE", cfg.retentionBatchSize); parseErr != nil {
		return runtimeConfig{}, parseErr
	}
	if claimSeconds, err := positiveIntEnv("RETENTION_CLAIM_SECONDS", int(cfg.retentionClaimDuration/time.Second)); err != nil {
		return runtimeConfig{}, err
	} else {
		cfg.retentionClaimDuration = time.Duration(claimSeconds) * time.Second
	}
	if graceMinutes, err := positiveIntEnv("OBJECT_ORPHAN_GRACE_MINUTES", int(cfg.objectOrphanGrace/time.Minute)); err != nil {
		return runtimeConfig{}, err
	} else {
		cfg.objectOrphanGrace = time.Duration(graceMinutes) * time.Minute
	}
	if deliveryHours, err := positiveIntEnv("EXPORT_DELIVERY_TTL_HOURS", int(cfg.exportDeliveryTTL/time.Hour)); err != nil {
		return runtimeConfig{}, err
	} else {
		cfg.exportDeliveryTTL = time.Duration(deliveryHours) * time.Hour
	}
	if webHours, err := positiveIntEnv("EXPORT_WEB_ACCESS_TTL_HOURS", int(cfg.exportWebAccessTTL/time.Hour)); err != nil {
		return runtimeConfig{}, err
	} else {
		cfg.exportWebAccessTTL = time.Duration(webHours) * time.Hour
	}
	if cfg.exportDeliveryMaxAttempts, parseErr = positiveIntEnv("EXPORT_DELIVERY_MAX_ATTEMPTS", cfg.exportDeliveryMaxAttempts); parseErr != nil {
		return runtimeConfig{}, parseErr
	}
	return cfg, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func runRetentionLoop(ctx context.Context, target api.TargetService, cfg runtimeConfig, logger *log.Logger) {
	ticker := time.NewTicker(cfg.retentionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runRetentionCycle(ctx, target, cfg, logger)
		}
	}
}

func runRetentionCycle(ctx context.Context, target api.TargetService, cfg runtimeConfig, logger *log.Logger) {
	if metadata, ok := target.(interface {
		ReclaimMetadataEnrichments(context.Context, int) (api.TargetMetadataEnrichmentReclaimResult, error)
	}); ok {
		reclaimed, err := metadata.ReclaimMetadataEnrichments(ctx, cfg.retentionBatchSize)
		if err != nil {
			logger.Printf("metadata_enrichment_lease_reclaim_failed error=%v", err)
		} else if reclaimed.Examined > 0 {
			logger.Printf(
				"metadata_enrichment_lease_reclaimed examined=%d requeued=%d failed=%d",
				reclaimed.Examined,
				reclaimed.Requeued,
				reclaimed.Failed,
			)
		}
	}
	reclaimedExports, err := target.ReclaimExportJobs(ctx, api.TargetExportReclaimRequest{BatchSize: cfg.retentionBatchSize})
	if err != nil {
		logger.Printf("export_lease_reclaim_failed error=%v", err)
	} else if reclaimedExports.Examined > 0 {
		logger.Printf(
			"export_lease_reclaimed examined=%d requeued=%d failed=%d",
			reclaimedExports.Examined,
			reclaimedExports.Requeued,
			reclaimedExports.Failed,
		)
	}
	reconciled, err := target.ReconcileRetention(ctx, api.TargetRetentionReconcileRequest{BatchSize: cfg.retentionBatchSize})
	if err != nil {
		logger.Printf("retention_reconcile_failed error=%v", err)
	} else if reconciled.OrphansDeleted > 0 || reconciled.PublicationsReconciled > 0 || reconciled.ObjectsMarkedMissing > 0 {
		logger.Printf(
			"retention_reconciled_orphan examined=%d deleted=%d publications=%d missing=%d",
			reconciled.Examined,
			reconciled.OrphansDeleted,
			reconciled.PublicationsReconciled,
			reconciled.ObjectsMarkedMissing,
		)
		if reconciled.ObjectsMarkedMissing > 0 {
			logger.Printf("stored_object_missing count=%d examined=%d", reconciled.ObjectsMarkedMissing, reconciled.Examined)
		}
	}
	result, err := target.SweepRetention(ctx, api.TargetRetentionSweepRequest{
		BatchSize: cfg.retentionBatchSize, DeletionOwner: "api-retention",
		ClaimSeconds: int(cfg.retentionClaimDuration / time.Second),
	})
	if err != nil {
		logger.Printf("retention_delete_failed error=%v", err)
		return
	}
	if result.Claimed > 0 {
		logger.Printf("retention_claimed claimed=%d deleted=%d failed=%d", result.Claimed, result.Deleted, result.Failed)
	}
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
		baseline := migrations[0].Name
		hasTargetSchema, err := schemaRelationExists(ctx, db, "public.channel_accounts")
		if err != nil {
			return err
		}
		if hasTargetSchema {
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
