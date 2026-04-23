package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/danila/telegram-transcriber-bot/apps/api/internal/storage"
)

const (
	EmitVersionedEventMarker = "[ApiEvents][emitJobEvent][BLOCK_EMIT_VERSIONED_EVENT]"
	ScheduleWebhookMarker    = "[ApiEvents][scheduleWebhook][BLOCK_EMIT_VERSIONED_EVENT]"
)

var (
	ErrEventDispatchFailed = errors.New("event_dispatch_failed")
	ErrInvalidEventState   = errors.New("invalid_event_state")
)

var webhookRetryBackoff = []time.Duration{
	0,
	10 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

type Logger interface {
	Printf(format string, args ...any)
}

type Store interface {
	AppendJobEvent(ctx context.Context, event storage.JobEvent) error
	CreateWebhookDelivery(ctx context.Context, delivery storage.WebhookDelivery) error
	UpdateWebhookDelivery(ctx context.Context, delivery storage.WebhookDelivery) error
}

type Broadcaster interface {
	Broadcast(ctx context.Context, envelope JobEventEnvelope) error
}

type Dispatcher interface {
	Schedule(ctx context.Context, delivery storage.WebhookDelivery) error
}

type WebhookDeliveryStore interface {
	UpdateWebhookDelivery(ctx context.Context, delivery storage.WebhookDelivery) error
	ListDueWebhookDeliveries(ctx context.Context, now time.Time, limit int) ([]storage.WebhookDelivery, error)
}

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type Service struct {
	store       Store
	broadcaster Broadcaster
	dispatcher  Dispatcher
	logger      Logger
	now         func() time.Time
	nextID      func() string
}

type Option func(*Service)

func WithLogger(logger Logger) Option {
	return func(s *Service) {
		s.logger = logger
	}
}

func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		s.now = now
	}
}

func WithIDGenerator(next func() string) Option {
	return func(s *Service) {
		s.nextID = next
	}
}

func NewService(store Store, broadcaster Broadcaster, dispatcher Dispatcher, opts ...Option) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidEventState)
	}

	service := &Service{
		store:       store,
		broadcaster: broadcaster,
		dispatcher:  dispatcher,
		now:         func() time.Time { return time.Now().UTC() },
		nextID:      uuid.NewString,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service, nil
}

type EventPayload struct {
	Status          string `json:"status"`
	ProgressStage   string `json:"progress_stage,omitempty"`
	ProgressMessage string `json:"progress_message,omitempty"`
}

type JobEventEnvelope struct {
	EventID   string       `json:"event_id"`
	EventType string       `json:"event_type"`
	JobID     string       `json:"job_id"`
	RootJobID string       `json:"root_job_id"`
	JobType   string       `json:"job_type"`
	Version   int64        `json:"version"`
	EmittedAt time.Time    `json:"emitted_at"`
	JobURL    string       `json:"job_url"`
	Payload   EventPayload `json:"payload"`
}

type EmitRequest struct {
	Job       storage.JobRecord
	EventType string
	JobURL    string
	Payload   EventPayload
}

type EmitResult struct {
	Event           storage.JobEvent
	Envelope        JobEventEnvelope
	WebhookDelivery *storage.WebhookDelivery
}

type DeliveryAttemptResult struct {
	HTTPStatus  int
	Err         error
	AttemptedAt time.Time
}

type HTTPWebhookDispatcher struct {
	store  WebhookDeliveryStore
	client HTTPDoer
	logger Logger
	now    func() time.Time
}

type WebhookDispatcherOption func(*HTTPWebhookDispatcher)

func WithWebhookHTTPClient(client HTTPDoer) WebhookDispatcherOption {
	return func(d *HTTPWebhookDispatcher) {
		d.client = client
	}
}

func WithWebhookLogger(logger Logger) WebhookDispatcherOption {
	return func(d *HTTPWebhookDispatcher) {
		d.logger = logger
	}
}

func WithWebhookClock(now func() time.Time) WebhookDispatcherOption {
	return func(d *HTTPWebhookDispatcher) {
		d.now = now
	}
}

func NewHTTPWebhookDispatcher(store WebhookDeliveryStore, opts ...WebhookDispatcherOption) (*HTTPWebhookDispatcher, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: webhook store is required", ErrInvalidEventState)
	}
	dispatcher := &HTTPWebhookDispatcher{
		store: store,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		now: func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(dispatcher)
	}
	if dispatcher.client == nil {
		return nil, fmt.Errorf("%w: webhook http client is required", ErrInvalidEventState)
	}
	return dispatcher, nil
}

func (s *Service) EmitJobEvent(ctx context.Context, req EmitRequest) (EmitResult, error) {
	if err := validateEmitRequest(req); err != nil {
		return EmitResult{}, err
	}

	envelope := JobEventEnvelope{
		EventID:   s.nextID(),
		EventType: req.EventType,
		JobID:     req.Job.ID,
		RootJobID: req.Job.RootJobID,
		JobType:   req.Job.JobType,
		Version:   req.Job.Version,
		EmittedAt: s.now(),
		JobURL:    req.JobURL,
		Payload:   req.Payload,
	}
	payloadJSON, err := json.Marshal(envelope)
	if err != nil {
		return EmitResult{}, fmt.Errorf("%w: encode event payload: %v", ErrEventDispatchFailed, err)
	}

	event := storage.JobEvent{
		ID:        envelope.EventID,
		JobID:     envelope.JobID,
		RootJobID: envelope.RootJobID,
		EventType: envelope.EventType,
		Version:   envelope.Version,
		Payload:   payloadJSON,
		CreatedAt: envelope.EmittedAt,
	}

	s.logf("%s job_id=%s event_type=%s version=%d", EmitVersionedEventMarker, req.Job.ID, req.EventType, req.Job.Version)

	if err := s.store.AppendJobEvent(ctx, event); err != nil {
		return EmitResult{}, fmt.Errorf("%w: append job event: %v", ErrEventDispatchFailed, err)
	}

	if s.broadcaster != nil {
		if err := s.broadcaster.Broadcast(ctx, envelope); err != nil {
			return EmitResult{}, fmt.Errorf("%w: broadcast event: %v", ErrEventDispatchFailed, err)
		}
	}

	var delivery *storage.WebhookDelivery
	if req.Job.Delivery.Strategy == storage.DeliveryStrategyWebhook {
		row := storage.WebhookDelivery{
			ID:            s.nextID(),
			JobEventID:    event.ID,
			JobID:         req.Job.ID,
			TargetURL:     req.Job.Delivery.WebhookURL,
			Payload:       payloadJSON,
			State:         storage.WebhookStatePending,
			AttemptCount:  0,
			NextAttemptAt: envelope.EmittedAt.Add(webhookRetryBackoff[0]),
			CreatedAt:     envelope.EmittedAt,
		}
		if err := s.store.CreateWebhookDelivery(ctx, row); err != nil {
			return EmitResult{}, fmt.Errorf("%w: persist webhook delivery: %v", ErrEventDispatchFailed, err)
		}
		if s.dispatcher != nil {
			if err := s.dispatcher.Schedule(ctx, row); err != nil {
				return EmitResult{}, fmt.Errorf("%w: schedule webhook delivery: %v", ErrEventDispatchFailed, err)
			}
		}
		delivery = &row
	}

	return EmitResult{
		Event:           event,
		Envelope:        envelope,
		WebhookDelivery: delivery,
	}, nil
}

func (d *HTTPWebhookDispatcher) Schedule(ctx context.Context, delivery storage.WebhookDelivery) error {
	attemptedAt := d.now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.TargetURL, bytes.NewReader(delivery.Payload))
	if err != nil {
		updated := ApplyWebhookAttempt(delivery, DeliveryAttemptResult{Err: err, AttemptedAt: attemptedAt})
		d.logf("%s delivery_id=%s state=%s attempt=%d error=%q", ScheduleWebhookMarker, updated.ID, updated.State, updated.AttemptCount, updated.LastError)
		return d.store.UpdateWebhookDelivery(ctx, updated)
	}
	req.Header.Set("Content-Type", "application/json")

	response, err := d.client.Do(req)
	if err != nil {
		updated := ApplyWebhookAttempt(delivery, DeliveryAttemptResult{Err: err, AttemptedAt: attemptedAt})
		d.logf("%s delivery_id=%s state=%s attempt=%d error=%q", ScheduleWebhookMarker, updated.ID, updated.State, updated.AttemptCount, updated.LastError)
		return d.store.UpdateWebhookDelivery(ctx, updated)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	updated := ApplyWebhookAttempt(delivery, DeliveryAttemptResult{HTTPStatus: response.StatusCode, AttemptedAt: attemptedAt})
	status := 0
	if updated.LastHTTPStatus != nil {
		status = *updated.LastHTTPStatus
	}
	d.logf("%s delivery_id=%s state=%s attempt=%d http_status=%d", ScheduleWebhookMarker, updated.ID, updated.State, updated.AttemptCount, status)
	return d.store.UpdateWebhookDelivery(ctx, updated)
}

func (d *HTTPWebhookDispatcher) DispatchDue(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("%w: due dispatch limit must be positive", ErrInvalidEventState)
	}
	deliveries, err := d.store.ListDueWebhookDeliveries(ctx, d.now(), limit)
	if err != nil {
		return 0, fmt.Errorf("%w: list due webhook deliveries: %v", ErrEventDispatchFailed, err)
	}
	for idx, delivery := range deliveries {
		if err := d.Schedule(ctx, delivery); err != nil {
			return idx, err
		}
	}
	return len(deliveries), nil
}

func (d *HTTPWebhookDispatcher) Run(ctx context.Context, interval time.Duration, limit int) {
	if interval <= 0 {
		interval = time.Second
	}
	if limit <= 0 {
		limit = 20
	}
	_, _ = d.DispatchDue(ctx, limit)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = d.DispatchDue(ctx, limit)
		}
	}
}

func ApplyWebhookAttempt(delivery storage.WebhookDelivery, result DeliveryAttemptResult) storage.WebhookDelivery {
	attemptedAt := result.AttemptedAt
	if attemptedAt.IsZero() {
		attemptedAt = time.Now().UTC()
	}
	delivery.AttemptCount++
	delivery.LastAttemptAt = &attemptedAt
	if result.Err != nil {
		return advanceRetry(delivery, attemptedAt, result.Err.Error(), 0)
	}
	delivery.LastHTTPStatus = &result.HTTPStatus

	switch {
	case result.HTTPStatus >= http.StatusOK && result.HTTPStatus < http.StatusMultipleChoices:
		delivery.State = storage.WebhookStateDelivered
		delivery.DeliveredAt = &attemptedAt
		delivery.NextAttemptAt = time.Time{}
	case isRetryableStatus(result.HTTPStatus):
		delivery = advanceRetry(delivery, attemptedAt, "", result.HTTPStatus)
	default:
		delivery.State = storage.WebhookStateDead
		delivery.NextAttemptAt = time.Time{}
	}
	return delivery
}

func RequiresRESTReconciliation(lastSeenVersion, incomingVersion int64) bool {
	if incomingVersion <= lastSeenVersion {
		return true
	}
	return incomingVersion != lastSeenVersion+1
}

func validateEmitRequest(req EmitRequest) error {
	if strings.TrimSpace(req.Job.ID) == "" || strings.TrimSpace(req.Job.RootJobID) == "" || strings.TrimSpace(req.Job.JobType) == "" || strings.TrimSpace(req.EventType) == "" || strings.TrimSpace(req.JobURL) == "" {
		return fmt.Errorf("%w: emit request requires job lineage, event type, and job url", ErrInvalidEventState)
	}
	if req.Job.Version < 1 {
		return fmt.Errorf("%w: job version must be >= 1", ErrInvalidEventState)
	}
	if req.Job.Delivery.Strategy == storage.DeliveryStrategyWebhook && strings.TrimSpace(req.Job.Delivery.WebhookURL) == "" {
		return fmt.Errorf("%w: webhook jobs require webhook url", ErrInvalidEventState)
	}
	return nil
}

func advanceRetry(delivery storage.WebhookDelivery, attemptedAt time.Time, lastError string, status int) storage.WebhookDelivery {
	delivery.LastError = lastError
	if status != 0 {
		delivery.LastHTTPStatus = &status
	}
	if delivery.AttemptCount >= len(webhookRetryBackoff) {
		delivery.State = storage.WebhookStateDead
		delivery.NextAttemptAt = time.Time{}
		return delivery
	}
	delivery.State = storage.WebhookStatePending
	delivery.NextAttemptAt = attemptedAt.Add(webhookRetryBackoff[delivery.AttemptCount])
	return delivery
}

func isRetryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func (s *Service) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func (d *HTTPWebhookDispatcher) logf(format string, args ...any) {
	if d.logger != nil {
		d.logger.Printf(format, args...)
	}
}
