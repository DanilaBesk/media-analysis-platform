package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/net/websocket"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

func TestApiEventsEmitPersistsBroadcastsAndSchedulesWebhook(t *testing.T) {
	t.Parallel()

	store := newEventMemoryStore()
	broadcaster := &fakeBroadcaster{}
	dispatcher := &fakeDispatcher{}
	logger := &eventsBufferLogger{}

	service := newEventTestService(t, store, broadcaster, dispatcher, WithLogger(logger))

	job := storage.JobRecord{
		ID:        "job-1",
		RootJobID: "job-1",
		JobType:   "transcription",
		Status:    "running",
		Version:   4,
		Delivery: storage.Delivery{
			Strategy:   storage.DeliveryStrategyWebhook,
			WebhookURL: "https://example.com/hook",
		},
		ProgressStage:   "rendering_artifacts",
		ProgressMessage: "almost done",
	}

	result, err := service.EmitJobEvent(context.Background(), EmitRequest{
		Job:       job,
		EventType: "job.updated",
		JobURL:    "/v1/jobs/job-1",
		Payload: EventPayload{
			Status:          "running",
			ProgressStage:   "rendering_artifacts",
			ProgressMessage: "almost done",
		},
	})
	if err != nil {
		t.Fatalf("EmitJobEvent() error = %v", err)
	}

	if got, want := len(store.events), 1; got != want {
		t.Fatalf("persisted event rows = %d, want %d", got, want)
	}
	if got, want := len(store.deliveries), 1; got != want {
		t.Fatalf("persisted webhook rows = %d, want %d", got, want)
	}
	if got, want := len(broadcaster.envelopes), 1; got != want {
		t.Fatalf("broadcast count = %d, want %d", got, want)
	}
	if got, want := len(dispatcher.deliveries), 1; got != want {
		t.Fatalf("scheduled webhook count = %d, want %d", got, want)
	}
	if result.WebhookDelivery == nil || result.WebhookDelivery.JobEventID != result.Event.ID {
		t.Fatalf("webhook delivery linkage = %#v, event=%#v", result.WebhookDelivery, result.Event)
	}
	if !strings.Contains(logger.String(), EmitVersionedEventMarker) {
		t.Fatalf("logger output missing marker %q", EmitVersionedEventMarker)
	}

	var payload map[string]any
	if err := json.Unmarshal(store.events[0].Payload, &payload); err != nil {
		t.Fatalf("Unmarshal(stored payload) error = %v", err)
	}
	if _, ok := payload["version"]; !ok {
		t.Fatalf("stored payload missing version: %#v", payload)
	}
}

func TestApiEventsDefaultIDGeneratorUsesUUIDs(t *testing.T) {
	t.Parallel()

	store := newEventMemoryStore()
	service, err := NewService(
		store,
		nil,
		nil,
		WithClock(func() time.Time { return time.Date(2026, 4, 23, 9, 30, 0, 0, time.UTC) }),
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result, err := service.EmitJobEvent(context.Background(), EmitRequest{
		Job: storage.JobRecord{
			ID:        uuid.NewString(),
			RootJobID: uuid.NewString(),
			JobType:   "transcription",
			Status:    "queued",
			Version:   1,
			Delivery: storage.Delivery{
				Strategy:   storage.DeliveryStrategyWebhook,
				WebhookURL: "https://example.com/hook",
			},
		},
		EventType: "job.created",
		JobURL:    "/v1/jobs/test",
		Payload: EventPayload{
			Status: "queued",
		},
	})
	if err != nil {
		t.Fatalf("EmitJobEvent() error = %v", err)
	}
	if result.WebhookDelivery == nil {
		t.Fatal("expected webhook delivery to be created")
	}

	for label, value := range map[string]string{
		"event_id":            result.Event.ID,
		"envelope_event_id":   result.Envelope.EventID,
		"webhook_delivery_id": result.WebhookDelivery.ID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			t.Fatalf("%s = %q, want UUID: %v", label, value, err)
		}
	}
}

func TestApiEventsDefaultClocksAdvance(t *testing.T) {
	t.Parallel()

	store := newEventMemoryStore()
	service, err := NewService(store, nil, nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	dispatcher, err := NewHTTPWebhookDispatcher(store)
	if err != nil {
		t.Fatalf("NewHTTPWebhookDispatcher() error = %v", err)
	}

	serviceFirst := service.now()
	dispatcherFirst := dispatcher.now()
	time.Sleep(2 * time.Millisecond)
	if serviceSecond := service.now(); !serviceSecond.After(serviceFirst) {
		t.Fatalf("service default clock did not advance: first=%v second=%v", serviceFirst, serviceSecond)
	}
	if dispatcherSecond := dispatcher.now(); !dispatcherSecond.After(dispatcherFirst) {
		t.Fatalf("dispatcher default clock did not advance: first=%v second=%v", dispatcherFirst, dispatcherSecond)
	}
}

func TestApiEventsWebhookRetryBackoffAndDeadLetter(t *testing.T) {
	t.Parallel()

	fixture := loadBackoffFixture(t)
	delivery := storage.WebhookDelivery{
		ID:            "delivery-1",
		State:         storage.WebhookStatePending,
		AttemptCount:  0,
		NextAttemptAt: fixture.BaseTime,
	}

	attempted := fixture.BaseTime
	for idx, backoff := range fixture.ExpectedBackoffs {
		delivery = ApplyWebhookAttempt(delivery, DeliveryAttemptResult{
			HTTPStatus:  httpStatusFromIndex(idx),
			Err:         fmt.Errorf("temporary failure %d", idx),
			AttemptedAt: attempted,
		})
		if idx < len(fixture.ExpectedBackoffs)-1 {
			if delivery.State != storage.WebhookStatePending {
				t.Fatalf("attempt %d state = %q, want pending", idx+1, delivery.State)
			}
			if got := delivery.NextAttemptAt.Sub(attempted); got != backoff {
				t.Fatalf("attempt %d backoff = %v, want %v", idx+1, got, backoff)
			}
		}
		attempted = attempted.Add(backoff)
	}

	delivery = ApplyWebhookAttempt(delivery, DeliveryAttemptResult{
		HTTPStatus:  502,
		AttemptedAt: attempted,
	})
	if delivery.State != storage.WebhookStateDead {
		t.Fatalf("exhausted retries should dead-letter delivery, got %q", delivery.State)
	}
}

func TestApiEventsHTTPWebhookDispatcherPersists5xxRetryAnd2xxDelivery(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	baseTime := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	now := baseTime
	store := newEventMemoryStore()
	store.deliveries = append(store.deliveries, storage.WebhookDelivery{
		ID:            "delivery-http-1",
		JobEventID:    "event-http-1",
		JobID:         "job-http-1",
		TargetURL:     server.URL,
		Payload:       []byte(`{"event_type":"job.updated"}`),
		State:         storage.WebhookStatePending,
		AttemptCount:  0,
		NextAttemptAt: baseTime,
		CreatedAt:     baseTime,
	})
	logger := &eventsBufferLogger{}
	dispatcher, err := NewHTTPWebhookDispatcher(
		store,
		WithWebhookHTTPClient(server.Client()),
		WithWebhookClock(func() time.Time { return now }),
		WithWebhookLogger(logger),
	)
	if err != nil {
		t.Fatalf("NewHTTPWebhookDispatcher() error = %v", err)
	}

	count, err := dispatcher.DispatchDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("first DispatchDue() error = %v", err)
	}
	if count != 1 || attempts.Load() != 1 {
		t.Fatalf("first dispatch count/attempts = %d/%d, want 1/1", count, attempts.Load())
	}
	delivery := store.deliveries[0]
	if delivery.State != storage.WebhookStatePending || delivery.AttemptCount != 1 {
		t.Fatalf("after 5xx delivery state = %q attempt=%d, want pending attempt=1", delivery.State, delivery.AttemptCount)
	}
	if delivery.LastHTTPStatus == nil || *delivery.LastHTTPStatus != http.StatusBadGateway {
		t.Fatalf("last status = %#v, want 502", delivery.LastHTTPStatus)
	}
	if got, want := delivery.NextAttemptAt.Sub(baseTime), 10*time.Second; got != want {
		t.Fatalf("next retry delay = %v, want %v", got, want)
	}

	now = baseTime.Add(9 * time.Second)
	count, err = dispatcher.DispatchDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("early DispatchDue() error = %v", err)
	}
	if count != 0 || attempts.Load() != 1 {
		t.Fatalf("early dispatch count/attempts = %d/%d, want 0/1", count, attempts.Load())
	}

	now = baseTime.Add(10 * time.Second)
	count, err = dispatcher.DispatchDue(context.Background(), 10)
	if err != nil {
		t.Fatalf("second DispatchDue() error = %v", err)
	}
	if count != 1 || attempts.Load() != 2 {
		t.Fatalf("second dispatch count/attempts = %d/%d, want 1/2", count, attempts.Load())
	}
	delivery = store.deliveries[0]
	if delivery.State != storage.WebhookStateDelivered || delivery.AttemptCount != 2 {
		t.Fatalf("after 2xx delivery state = %q attempt=%d, want delivered attempt=2", delivery.State, delivery.AttemptCount)
	}
	if delivery.DeliveredAt == nil || !delivery.DeliveredAt.Equal(now) {
		t.Fatalf("delivered_at = %v, want %v", delivery.DeliveredAt, now)
	}
	if !strings.Contains(logger.String(), ScheduleWebhookMarker) {
		t.Fatalf("logger output missing marker %q: %s", ScheduleWebhookMarker, logger.String())
	}
}

func TestApiEventsReconnectRequiresRestReconciliationOnVersionGap(t *testing.T) {
	t.Parallel()

	cases := []struct {
		lastSeen int64
		incoming int64
		want     bool
	}{
		{lastSeen: 3, incoming: 4, want: false},
		{lastSeen: 3, incoming: 3, want: true},
		{lastSeen: 3, incoming: 5, want: true},
		{lastSeen: 0, incoming: 1, want: false},
	}

	for _, tc := range cases {
		if got := RequiresRESTReconciliation(tc.lastSeen, tc.incoming); got != tc.want {
			t.Fatalf("RequiresRESTReconciliation(%d, %d) = %v, want %v", tc.lastSeen, tc.incoming, got, tc.want)
		}
	}
}

func TestApiEventsHubBroadcastsEnvelopeToConnectedWebsocketClients(t *testing.T) {
	t.Parallel()

	hub := NewHub()
	server := httptest.NewServer(hub)
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, err := websocket.Dial(wsURL, "", "http://localhost:3000")
	if err != nil {
		t.Fatalf("Dial(%s) error = %v", wsURL, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	envelope := JobEventEnvelope{
		EventID:   "11111111-1111-1111-1111-111111111111",
		EventType: "job.updated",
		JobID:     "22222222-2222-2222-2222-222222222222",
		RootJobID: "22222222-2222-2222-2222-222222222222",
		JobType:   "transcription",
		Version:   2,
		EmittedAt: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
		JobURL:    "/v1/jobs/22222222-2222-2222-2222-222222222222",
		Payload: EventPayload{
			Status:        "running",
			ProgressStage: "transcribing",
		},
	}
	if err := hub.Broadcast(context.Background(), envelope); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	var got JobEventEnvelope
	if err := websocket.JSON.Receive(conn, &got); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if got.EventID != envelope.EventID || got.Version != envelope.Version || got.Payload.ProgressStage != envelope.Payload.ProgressStage {
		t.Fatalf("received envelope = %#v, want %#v", got, envelope)
	}
}

func TestApiEventsBackoffFixtureShape(t *testing.T) {
	t.Parallel()

	fixture := loadBackoffFixture(t)
	if len(fixture.ExpectedBackoffs) != 5 {
		t.Fatalf("expected 5 bounded retry intervals, got %d", len(fixture.ExpectedBackoffs))
	}
}

func loadBackoffFixture(t *testing.T) webhookFixture {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "webhook_backoff.json"))
	if err != nil {
		t.Fatalf("ReadFile(testdata/webhook_backoff.json) error = %v", err)
	}

	var raw struct {
		BaseTime        string   `json:"base_time"`
		ExpectedBackoff []string `json:"expected_backoff"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal(webhook_backoff.json) error = %v", err)
	}

	baseTime, err := time.Parse(time.RFC3339, raw.BaseTime)
	if err != nil {
		t.Fatalf("Parse(base_time) error = %v", err)
	}

	backoffs := make([]time.Duration, 0, len(raw.ExpectedBackoff))
	for _, item := range raw.ExpectedBackoff {
		backoff, err := time.ParseDuration(item)
		if err != nil {
			t.Fatalf("ParseDuration(%q) error = %v", item, err)
		}
		backoffs = append(backoffs, backoff)
	}
	return webhookFixture{BaseTime: baseTime, ExpectedBackoffs: backoffs}
}

type webhookFixture struct {
	BaseTime         time.Time
	ExpectedBackoffs []time.Duration
}

type eventMemoryStore struct {
	events     []storage.JobEvent
	deliveries []storage.WebhookDelivery
}

func newEventMemoryStore() *eventMemoryStore {
	return &eventMemoryStore{}
}

func (s *eventMemoryStore) AppendJobEvent(_ context.Context, event storage.JobEvent) error {
	for _, existing := range s.events {
		if existing.JobID == event.JobID && existing.Version == event.Version {
			return fmt.Errorf("duplicate job version")
		}
	}
	s.events = append(s.events, event)
	return nil
}

func (s *eventMemoryStore) CreateWebhookDelivery(_ context.Context, delivery storage.WebhookDelivery) error {
	s.deliveries = append(s.deliveries, delivery)
	return nil
}

func (s *eventMemoryStore) UpdateWebhookDelivery(_ context.Context, delivery storage.WebhookDelivery) error {
	for idx := range s.deliveries {
		if s.deliveries[idx].ID == delivery.ID {
			s.deliveries[idx] = delivery
			return nil
		}
	}
	s.deliveries = append(s.deliveries, delivery)
	return nil
}

func (s *eventMemoryStore) ListDueWebhookDeliveries(_ context.Context, now time.Time, limit int) ([]storage.WebhookDelivery, error) {
	due := make([]storage.WebhookDelivery, 0, limit)
	for _, delivery := range s.deliveries {
		if delivery.State == storage.WebhookStatePending && !delivery.NextAttemptAt.After(now) {
			due = append(due, delivery)
			if len(due) == limit {
				break
			}
		}
	}
	return due, nil
}

type fakeBroadcaster struct {
	envelopes []JobEventEnvelope
}

func (b *fakeBroadcaster) Broadcast(_ context.Context, envelope JobEventEnvelope) error {
	b.envelopes = append(b.envelopes, envelope)
	return nil
}

type fakeDispatcher struct {
	deliveries []storage.WebhookDelivery
}

func (d *fakeDispatcher) Schedule(_ context.Context, delivery storage.WebhookDelivery) error {
	d.deliveries = append(d.deliveries, delivery)
	return nil
}

type eventsBufferLogger struct {
	lines []string
}

func (l *eventsBufferLogger) Printf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *eventsBufferLogger) String() string {
	return strings.Join(l.lines, "\n")
}

func newEventTestService(t *testing.T, store *eventMemoryStore, broadcaster Broadcaster, dispatcher Dispatcher, opts ...Option) *Service {
	t.Helper()

	index := 0
	base := []Option{
		WithClock(func() time.Time { return time.Date(2026, 4, 22, 14, 0, 0, 0, time.UTC) }),
		WithIDGenerator(func() string {
			index++
			return fmt.Sprintf("evt-%02d", index)
		}),
	}
	base = append(base, opts...)

	service, err := NewService(store, broadcaster, dispatcher, base...)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func httpStatusFromIndex(index int) int {
	if index%2 == 0 {
		return 502
	}
	return 429
}
