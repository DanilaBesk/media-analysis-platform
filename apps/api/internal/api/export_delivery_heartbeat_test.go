package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

type exportDeliveryHeartbeatService struct {
	fakeTargetService
	req TargetHeartbeatExportDeliveryRequest
}

func heartbeatLeaseSeconds(value int) *int {
	return &value
}

func (f *fakeTargetService) HeartbeatExportDelivery(context.Context, TargetHeartbeatExportDeliveryRequest) (TargetExportDeliveryClaim, error) {
	return TargetExportDeliveryClaim{}, nil
}

func (f *exportDeliveryHeartbeatService) HeartbeatExportDelivery(_ context.Context, req TargetHeartbeatExportDeliveryRequest) (TargetExportDeliveryClaim, error) {
	f.req = req
	expiresAt := time.Date(2026, 7, 26, 12, 5, 0, 0, time.UTC)
	return TargetExportDeliveryClaim{
		Delivery: TargetExportDelivery{
			ExportDeliveryID: req.ExportDeliveryID,
			ExportJobID:      req.ExportJobID,
			ChannelAccountID: req.ChannelAccountID,
			Status:           "claimed",
			LeaseExpiresAt:   &expiresAt,
		},
		AttemptToken:   req.AttemptToken,
		LeaseOwner:     req.LeaseOwner,
		LeaseExpiresAt: expiresAt,
	}, nil
}

func TestExportDeliveryHeartbeatRouteForwardsCurrentFence(t *testing.T) {
	target := &exportDeliveryHeartbeatService{}
	mux := http.NewServeMux()
	NewServer(Dependencies{Target: target}).RegisterRoutes(mux)

	recorder := httptest.NewRecorder()
	request := jsonRequest(http.MethodPost, "/v1/export-jobs/export-job-1/deliveries/heartbeat", map[string]any{
		"channel_account_id": "channel-account-1",
		"export_delivery_id": "export-delivery-1",
		"lease_owner":        "telegram-adapter-1",
		"attempt_token":      "attempt-token-current",
		"lease_seconds":      300,
	})
	mux.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("heartbeat status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if target.req.ExportJobID != "export-job-1" || target.req.ChannelAccountID != "channel-account-1" ||
		target.req.ExportDeliveryID != "export-delivery-1" || target.req.LeaseOwner != "telegram-adapter-1" ||
		target.req.AttemptToken != "attempt-token-current" || target.req.LeaseSeconds == nil || *target.req.LeaseSeconds != 300 {
		t.Fatalf("heartbeat request = %#v", target.req)
	}
	var response TargetExportDeliveryClaim
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode heartbeat response: %v", err)
	}
	if response.AttemptToken != target.req.AttemptToken || response.LeaseOwner != target.req.LeaseOwner {
		t.Fatalf("heartbeat response = %#v", response)
	}
}

type exportDeliveryHeartbeatStore struct {
	fakeTargetRuntimeStore
	params targetstore.HeartbeatExportDeliveryParams
	err    error
}

func (s *fakeTargetRuntimeStore) HeartbeatExportDelivery(context.Context, targetstore.HeartbeatExportDeliveryParams) (targetstore.ExportDeliveryRecord, error) {
	return targetstore.ExportDeliveryRecord{}, sql.ErrNoRows
}

func (s *exportDeliveryHeartbeatStore) HeartbeatExportDelivery(_ context.Context, params targetstore.HeartbeatExportDeliveryParams) (targetstore.ExportDeliveryRecord, error) {
	s.params = params
	if s.err != nil {
		return targetstore.ExportDeliveryRecord{}, s.err
	}
	return targetstore.ExportDeliveryRecord{
		ID:               params.ExportDeliveryID,
		ExportJobID:      params.ExportJobID,
		ChannelAccountID: params.ChannelAccountID,
		Status:           "claimed",
		AttemptToken:     params.AttemptToken,
		LeaseOwner:       params.LeaseOwner,
		LeaseExpiresAt:   &params.LeaseExpiresAt,
	}, nil
}

func TestHeartbeatExportDeliveryExtendsCurrentFence(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	store := &exportDeliveryHeartbeatStore{}
	service := NewTargetRuntimeService(store, WithTargetObjectStore(&fakeTargetObjectStore{}), WithTargetClock(func() time.Time { return now }))

	claim, err := service.HeartbeatExportDelivery(context.Background(), TargetHeartbeatExportDeliveryRequest{
		ChannelAccountID: "channel-account-1",
		ExportJobID:      "export-job-1",
		ExportDeliveryID: "export-delivery-1",
		LeaseOwner:       "telegram-adapter-1",
		AttemptToken:     "attempt-token-current",
		LeaseSeconds:     heartbeatLeaseSeconds(300),
	})
	if err != nil {
		t.Fatalf("HeartbeatExportDelivery() error = %v", err)
	}
	if store.params.HeartbeatAt != now || store.params.LeaseExpiresAt != now.Add(5*time.Minute) {
		t.Fatalf("heartbeat params = %#v", store.params)
	}
	if claim.AttemptToken != "attempt-token-current" || claim.LeaseOwner != "telegram-adapter-1" ||
		claim.LeaseExpiresAt != now.Add(5*time.Minute) {
		t.Fatalf("heartbeat claim = %#v", claim)
	}

	defaultStore := &exportDeliveryHeartbeatStore{}
	defaultService := NewTargetRuntimeService(defaultStore, WithTargetObjectStore(&fakeTargetObjectStore{}), WithTargetClock(func() time.Time { return now }))
	if _, err := defaultService.HeartbeatExportDelivery(context.Background(), TargetHeartbeatExportDeliveryRequest{
		ChannelAccountID: "channel-account-1", ExportJobID: "export-job-1", ExportDeliveryID: "export-delivery-1",
		LeaseOwner: "telegram-adapter-1", AttemptToken: "attempt-token-current",
	}); err != nil {
		t.Fatalf("HeartbeatExportDelivery(default lease) error = %v", err)
	}
	if defaultStore.params.LeaseExpiresAt != now.Add(2*time.Minute) {
		t.Fatalf("default heartbeat lease expiry = %v, want %v", defaultStore.params.LeaseExpiresAt, now.Add(2*time.Minute))
	}
}

func TestHeartbeatExportDeliveryRejectsInvalidOrStaleFence(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		req  TargetHeartbeatExportDeliveryRequest
	}{
		{name: "missing account", req: TargetHeartbeatExportDeliveryRequest{ExportJobID: "job", ExportDeliveryID: "delivery", LeaseOwner: "owner", AttemptToken: "attempt-token-current"}},
		{name: "missing delivery", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", LeaseOwner: "owner", AttemptToken: "attempt-token-current"}},
		{name: "missing owner", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery", AttemptToken: "attempt-token-current"}},
		{name: "missing token", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery", LeaseOwner: "owner"}},
		{name: "zero lease", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery", LeaseOwner: "owner", AttemptToken: "attempt-token-current", LeaseSeconds: heartbeatLeaseSeconds(0)}},
		{name: "lease too large", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery", LeaseOwner: "owner", AttemptToken: "attempt-token-current", LeaseSeconds: heartbeatLeaseSeconds(901)}},
		{name: "negative lease", req: TargetHeartbeatExportDeliveryRequest{ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery", LeaseOwner: "owner", AttemptToken: "attempt-token-current", LeaseSeconds: heartbeatLeaseSeconds(-1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &exportDeliveryHeartbeatStore{}
			service := NewTargetRuntimeService(store, WithTargetObjectStore(&fakeTargetObjectStore{}), WithTargetClock(func() time.Time { return now }))
			if _, err := service.HeartbeatExportDelivery(context.Background(), test.req); !errors.Is(err, storage.ErrContractViolation) {
				t.Fatalf("HeartbeatExportDelivery() error = %v, want contract violation", err)
			}
		})
	}

	store := &exportDeliveryHeartbeatStore{err: sql.ErrNoRows}
	service := NewTargetRuntimeService(store, WithTargetObjectStore(&fakeTargetObjectStore{}), WithTargetClock(func() time.Time { return now }))
	_, err := service.HeartbeatExportDelivery(context.Background(), TargetHeartbeatExportDeliveryRequest{
		ChannelAccountID: "account", ExportJobID: "job", ExportDeliveryID: "delivery",
		LeaseOwner: "owner", AttemptToken: "attempt-token-current", LeaseSeconds: heartbeatLeaseSeconds(120),
	})
	if !errors.Is(err, storage.ErrExportJobConflict) {
		t.Fatalf("HeartbeatExportDelivery(stale fence) error = %v, want conflict", err)
	}
}
