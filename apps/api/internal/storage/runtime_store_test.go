package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestNewSQLStateStoreRequiresDB(t *testing.T) {
	t.Parallel()

	if _, err := NewSQLStateStore(nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewSQLStateStore(nil) error = %v, want ErrContractViolation", err)
	}

	db := openRuntimeStoreTestDB(t, &runtimeStoreTestDriverConfig{})
	store, err := NewSQLStateStore(db)
	if err != nil {
		t.Fatalf("NewSQLStateStore(db) error = %v", err)
	}
	if store.db != db {
		t.Fatalf("store.db = %p, want %p", store.db, db)
	}
}

func TestOpenPostgresDBValidatesInputsAndPingFailures(t *testing.T) {
	if _, err := OpenPostgresDB(context.Background(), "   "); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("OpenPostgresDB(blank) error = %v, want ErrContractViolation", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	db, err := OpenPostgresDB(ctx, "postgres://user:pass@127.0.0.1:1/test?sslmode=disable")
	if db != nil {
		t.Fatalf("OpenPostgresDB(unreachable) db = %#v, want nil", db)
	}
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("OpenPostgresDB(unreachable) error = %v, want ErrStorageUnavailable", err)
	}

	originalOpen := sqlOpenPostgres
	t.Cleanup(func() { sqlOpenPostgres = originalOpen })

	sqlOpenPostgres = func(driverName, dsn string) (*sql.DB, error) {
		if dsn == "open-error" {
			return nil, fmt.Errorf("boom")
		}
		return openRuntimeStoreTestDB(t, &runtimeStoreTestDriverConfig{}), nil
	}

	if _, err := OpenPostgresDB(context.Background(), "open-error"); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("OpenPostgresDB(open error) error = %v, want ErrStorageUnavailable", err)
	}
	okDB, err := OpenPostgresDB(context.Background(), "ok")
	if err != nil {
		t.Fatalf("OpenPostgresDB(success) error = %v", err)
	}
	if okDB == nil {
		t.Fatalf("OpenPostgresDB(success) db = nil, want non-nil")
	}
}

func TestNewMinioClientValidatesAndNormalizesEndpoints(t *testing.T) {
	if _, err := NewMinioClient("://bad", "key", "secret"); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewMinioClient(parse error) = %v, want ErrContractViolation", err)
	}
	if _, err := NewMinioClient("http://", "key", "secret"); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewMinioClient(missing host) = %v, want ErrContractViolation", err)
	}
	if _, err := NewMinioClient("http://localhost:9000", "", "secret"); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewMinioClient(missing credentials) = %v, want ErrContractViolation", err)
	}

	secure, err := NewMinioClient("https://localhost:9443", "key", "secret")
	if err != nil {
		t.Fatalf("NewMinioClient(https endpoint) error = %v", err)
	}
	if got := secure.EndpointURL(); got == nil || got.Host != "localhost:9443" || got.Scheme != "https" {
		t.Fatalf("https endpoint = %#v, want https://localhost:9443", got)
	}

	originalNewMinio := newMinioSDK
	t.Cleanup(func() { newMinioSDK = originalNewMinio })
	newMinioSDK = func(endpoint string, opts *minio.Options) (*minio.Client, error) {
		return nil, fmt.Errorf("boom")
	}
	if _, err := NewMinioClient("http://localhost:9000", "key", "secret"); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("NewMinioClient(create error) = %v, want ErrStorageUnavailable", err)
	}
}

func TestNewMinioObjectStoreRequiresClients(t *testing.T) {
	t.Parallel()

	if _, err := NewMinioObjectStore(nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewMinioObjectStore(nil) error = %v, want ErrContractViolation", err)
	}
	if _, err := NewMinioObjectStoreWithPresignClient(nil, nil); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("NewMinioObjectStoreWithPresignClient(nil, nil) error = %v, want ErrContractViolation", err)
	}
}

func TestMinioObjectStorePresignHostsAndObjectMethodErrors(t *testing.T) {
	internalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
	}))
	defer internalServer.Close()

	publicServer := httptest.NewServer(http.NotFoundHandler())
	defer publicServer.Close()

	internalClient := newRuntimeStoreMinioClient(t, internalServer.URL)
	publicClient := newRuntimeStoreMinioClient(t, publicServer.URL)

	store, err := NewMinioObjectStoreWithPresignClient(internalClient, publicClient)
	if err != nil {
		t.Fatalf("NewMinioObjectStoreWithPresignClient() error = %v", err)
	}

	publicURL, publicExpiry, err := store.PresignGetObject(context.Background(), "artifacts", "reports/output.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetObject() error = %v", err)
	}
	internalURL, internalExpiry, err := store.PresignInternalGetObject(context.Background(), "artifacts", "reports/output.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignInternalGetObject() error = %v", err)
	}

	parsedPublicURL, err := url.Parse(publicURL)
	if err != nil {
		t.Fatalf("Parse(publicURL) error = %v", err)
	}
	parsedInternalURL, err := url.Parse(internalURL)
	if err != nil {
		t.Fatalf("Parse(internalURL) error = %v", err)
	}
	if parsedPublicURL.Host != strings.TrimPrefix(publicServer.URL, "http://") {
		t.Fatalf("public presign host = %q, want %q", parsedPublicURL.Host, strings.TrimPrefix(publicServer.URL, "http://"))
	}
	if parsedInternalURL.Host != strings.TrimPrefix(internalServer.URL, "http://") {
		t.Fatalf("internal presign host = %q, want %q", parsedInternalURL.Host, strings.TrimPrefix(internalServer.URL, "http://"))
	}
	if time.Until(publicExpiry) <= 0 || time.Until(internalExpiry) <= 0 {
		t.Fatalf("expiry timestamps must be in the future: public=%s internal=%s", publicExpiry, internalExpiry)
	}

	internalServer.Close()
	if err := store.PutObject(context.Background(), "artifacts", "reports/output.txt", "text/plain", []byte("payload")); err == nil {
		t.Fatalf("PutObject() error = nil, want network failure")
	}
	if err := store.DeleteObject(context.Background(), "artifacts", "reports/output.txt"); err == nil {
		t.Fatalf("DeleteObject() error = nil, want network failure")
	}
}

func TestMinioObjectStorePresignErrorBranches(t *testing.T) {
	t.Parallel()

	client := newRuntimeStoreMinioClient(t, "http://localhost:9000")
	store, err := NewMinioObjectStore(client)
	if err != nil {
		t.Fatalf("NewMinioObjectStore() error = %v", err)
	}

	if _, _, err := store.PresignGetObject(context.Background(), "artifacts", "", time.Minute); err == nil {
		t.Fatalf("PresignGetObject(empty object) error = nil, want validation failure")
	}
	if _, _, err := store.PresignInternalGetObject(context.Background(), "artifacts", "", time.Minute); err == nil {
		t.Fatalf("PresignInternalGetObject(empty object) error = nil, want validation failure")
	}
}

func TestWithTxHandlesBeginRollbackCommitAndSuccess(t *testing.T) {
	t.Parallel()

	beginFailure := errors.New("begin failed")
	beginConfig := &runtimeStoreTestDriverConfig{beginErr: beginFailure}
	beginDB := openRuntimeStoreTestDB(t, beginConfig)
	if err := withTx(context.Background(), beginDB, func(*sql.Tx) error { return nil }); !errors.Is(err, beginFailure) {
		t.Fatalf("withTx(begin failure) error = %v, want %v", err, beginFailure)
	}
	if got := beginConfig.beginCalls.Load(); got != 1 {
		t.Fatalf("begin calls = %d, want 1", got)
	}

	fnFailure := errors.New("fn failed")
	rollbackConfig := &runtimeStoreTestDriverConfig{}
	rollbackDB := openRuntimeStoreTestDB(t, rollbackConfig)
	if err := withTx(context.Background(), rollbackDB, func(*sql.Tx) error { return fnFailure }); !errors.Is(err, fnFailure) {
		t.Fatalf("withTx(fn failure) error = %v, want %v", err, fnFailure)
	}
	if got := rollbackConfig.rollbackCalls.Load(); got != 1 {
		t.Fatalf("rollback calls = %d, want 1", got)
	}
	if got := rollbackConfig.commitCalls.Load(); got != 0 {
		t.Fatalf("commit calls after rollback = %d, want 0", got)
	}

	commitFailure := errors.New("commit failed")
	commitConfig := &runtimeStoreTestDriverConfig{commitErr: commitFailure}
	commitDB := openRuntimeStoreTestDB(t, commitConfig)
	if err := withTx(context.Background(), commitDB, func(*sql.Tx) error { return nil }); !errors.Is(err, commitFailure) {
		t.Fatalf("withTx(commit failure) error = %v, want %v", err, commitFailure)
	}
	if got := commitConfig.commitCalls.Load(); got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}

	successConfig := &runtimeStoreTestDriverConfig{}
	successDB := openRuntimeStoreTestDB(t, successConfig)
	if err := withTx(context.Background(), successDB, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("withTx(success) error = %v", err)
	}
	if got := successConfig.commitCalls.Load(); got != 1 {
		t.Fatalf("success commit calls = %d, want 1", got)
	}
	if got := successConfig.rollbackCalls.Load(); got != 0 {
		t.Fatalf("success rollback calls = %d, want 0", got)
	}
}

type runtimeStoreTestDriverConfig struct {
	beginErr      error
	commitErr     error
	rollbackErr   error
	pingErr       error
	beginCalls    atomic.Int64
	commitCalls   atomic.Int64
	rollbackCalls atomic.Int64
	pingCalls     atomic.Int64
}

type runtimeStoreTestDriver struct {
	config *runtimeStoreTestDriverConfig
}

type runtimeStoreTestConn struct {
	config *runtimeStoreTestDriverConfig
}

type runtimeStoreTestTx struct {
	config *runtimeStoreTestDriverConfig
}

var runtimeStoreTestDriverSeq atomic.Int64

func openRuntimeStoreTestDB(t *testing.T, config *runtimeStoreTestDriverConfig) *sql.DB {
	t.Helper()

	name := "runtime-store-test-" + strings.ReplaceAll(t.Name(), "/", "-") + "-" + strconv.FormatInt(runtimeStoreTestDriverSeq.Add(1), 10)
	sql.Register(name, &runtimeStoreTestDriver{config: config})

	db, err := sql.Open(name, "ignored")
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", name, err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func newRuntimeStoreMinioClient(t *testing.T, endpoint string) *minio.Client {
	t.Helper()

	client, err := NewMinioClient(endpoint, "minioadmin", "minioadmin")
	if err != nil {
		t.Fatalf("NewMinioClient(%q) error = %v", endpoint, err)
	}
	return client
}

func (d *runtimeStoreTestDriver) Open(string) (driver.Conn, error) {
	return &runtimeStoreTestConn{config: d.config}, nil
}

func (c *runtimeStoreTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *runtimeStoreTestConn) Close() error {
	return nil
}

func (c *runtimeStoreTestConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *runtimeStoreTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.config.beginCalls.Add(1)
	if c.config.beginErr != nil {
		return nil, c.config.beginErr
	}
	return &runtimeStoreTestTx{config: c.config}, nil
}

func (c *runtimeStoreTestConn) Ping(context.Context) error {
	c.config.pingCalls.Add(1)
	return c.config.pingErr
}

func (tx *runtimeStoreTestTx) Commit() error {
	tx.config.commitCalls.Add(1)
	return tx.config.commitErr
}

func (tx *runtimeStoreTestTx) Rollback() error {
	tx.config.rollbackCalls.Add(1)
	return tx.config.rollbackErr
}

var _ driver.Driver = (*runtimeStoreTestDriver)(nil)
var _ driver.Conn = (*runtimeStoreTestConn)(nil)
var _ driver.ConnBeginTx = (*runtimeStoreTestConn)(nil)
var _ driver.Pinger = (*runtimeStoreTestConn)(nil)
var _ driver.Tx = (*runtimeStoreTestTx)(nil)
