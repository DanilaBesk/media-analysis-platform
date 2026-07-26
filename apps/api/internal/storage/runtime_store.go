package storage

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	sqlOpenPostgres = sql.Open
	newMinioSDK     = minio.New
)

type minioObjectClient interface {
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	CopyObject(ctx context.Context, dst minio.CopyDestOptions, src minio.CopySrcOptions) (minio.UploadInfo, error)
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	PresignedGetObject(ctx context.Context, bucketName, objectName string, expires time.Duration, reqParams url.Values) (*url.URL, error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	EndpointURL() *url.URL
}

type MinioObjectStore struct {
	client        minioObjectClient
	presignClient minioObjectClient
}

func OpenPostgresDB(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: postgres dsn is required", ErrContractViolation)
	}
	db, err := sqlOpenPostgres("pgx", dsn)
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
	client, err := newMinioSDK(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: "us-east-1",
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

func (s *MinioObjectStore) PutObjectStream(ctx context.Context, bucket, objectKey, contentType string, reader io.Reader, sizeBytes int64, metadata map[string]string) error {
	_, err := s.client.PutObject(ctx, bucket, objectKey, reader, sizeBytes, minio.PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: metadata,
	})
	return err
}

func (s *MinioObjectStore) PresignGetObject(ctx context.Context, bucket, objectKey string, expiry time.Duration) (string, time.Time, error) {
	expiresAt := time.Now().UTC().Add(expiry)
	url, err := s.presignClient.PresignedGetObject(ctx, bucket, objectKey, expiry, nil)
	if err != nil {
		return "", time.Time{}, err
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

func (s *MinioObjectStore) PromoteObject(ctx context.Context, bucket, stagingKey, objectKey string, metadata map[string]string) error {
	destination := minio.CopyDestOptions{Bucket: bucket, Object: objectKey}
	if len(metadata) > 0 {
		destination.UserMetadata = metadata
		destination.ReplaceMetadata = true
	}
	if _, err := s.client.CopyObject(ctx,
		destination,
		minio.CopySrcOptions{Bucket: bucket, Object: stagingKey},
	); err != nil {
		return err
	}
	return s.DeleteObject(ctx, bucket, stagingKey)
}

func (s *MinioObjectStore) StatObject(ctx context.Context, bucket, objectKey string) (ManagedObjectInfo, error) {
	info, err := s.client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		response := minio.ToErrorResponse(err)
		if response.Code == "NoSuchKey" || response.Code == "NoSuchObject" || response.Code == "NotFound" {
			return ManagedObjectInfo{}, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, objectKey)
		}
		return ManagedObjectInfo{}, err
	}
	return ManagedObjectInfo{
		SizeBytes:   info.Size,
		ContentType: info.ContentType,
		ETag:        info.ETag,
		Metadata:    info.UserMetadata,
	}, nil
}

func (s *MinioObjectStore) ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) ([]ManagedObjectEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	entries := make([]ManagedObjectEntry, 0, limit)
	for info := range s.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, StartAfter: startAfter}) {
		if info.Err != nil {
			return nil, info.Err
		}
		entries = append(entries, ManagedObjectEntry{
			Bucket: bucket, ObjectKey: info.Key, SizeBytes: info.Size, LastModified: info.LastModified,
		})
		if len(entries) >= limit {
			break
		}
	}
	return entries, nil
}

var _ ObjectStore = (*MinioObjectStore)(nil)
var _ ManagedObjectStore = (*MinioObjectStore)(nil)
