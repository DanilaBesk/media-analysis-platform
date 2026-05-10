package storage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type SQLStateStore struct {
	db *sql.DB
}

type MinioObjectStore struct {
	client        *minio.Client
	presignClient *minio.Client
}

func NewSQLStateStore(db *sql.DB) (*SQLStateStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db is required", ErrContractViolation)
	}
	return &SQLStateStore{db: db}, nil
}

func OpenPostgresDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: postgres dsn is required", ErrContractViolation)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: open postgres: %v", ErrStorageUnavailable, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: ping postgres: %v", ErrStorageUnavailable, err)
	}
	return db, nil
}

func NewMinioClient(endpoint, accessKey, secretKey string) (*minio.Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, fmt.Errorf("%w: parse minio endpoint: %v", ErrContractViolation, err)
	}
	secure := parsed.Scheme == "https"
	host := parsed.Host
	if host == "" {
		host = parsed.Path
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(accessKey) == "" || strings.TrimSpace(secretKey) == "" {
		return nil, fmt.Errorf("%w: minio endpoint and credentials are required", ErrContractViolation)
	}
	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: create minio client: %v", ErrStorageUnavailable, err)
	}
	return client, nil
}

func NewMinioObjectStore(client *minio.Client) (*MinioObjectStore, error) {
	return NewMinioObjectStoreWithPresignClient(client, client)
}

func NewMinioObjectStoreWithPresignClient(client *minio.Client, presignClient *minio.Client) (*MinioObjectStore, error) {
	if client == nil || presignClient == nil {
		return nil, fmt.Errorf("%w: minio clients are required", ErrContractViolation)
	}
	return &MinioObjectStore{client: client, presignClient: presignClient}, nil
}

func (s *MinioObjectStore) PutObject(ctx context.Context, bucket, objectKey, contentType string, body []byte) error {
	_, err := s.client.PutObject(ctx, bucket, objectKey, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *MinioObjectStore) PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().UTC().Add(expiry)
	url, err := s.client.PresignedGetObject(ctx, bucket, objectKey, expiry, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	if endpoint := s.presignClient.EndpointURL(); endpoint != nil {
		url.Scheme = endpoint.Scheme
		url.Host = endpoint.Host
	}
	return url.String(), expiresAt, nil
}

func (s *MinioObjectStore) PresignInternalGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().UTC().Add(expiry)
	url, err := s.client.PresignedGetObject(ctx, bucket, objectKey, expiry, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	return url.String(), expiresAt, nil
}

func (s *MinioObjectStore) DeleteObject(ctx context.Context, bucket, objectKey string) error {
	return s.client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{})
}

func withTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

var _ MediaStateStore = (*SQLStateStore)(nil)
var _ ObjectStore = (*MinioObjectStore)(nil)
var _ ObjectDeleter = (*MinioObjectStore)(nil)
var _ internalObjectPresigner = (*MinioObjectStore)(nil)
