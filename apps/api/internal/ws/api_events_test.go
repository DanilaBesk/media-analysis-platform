package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
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
