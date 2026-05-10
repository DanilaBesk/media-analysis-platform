package ws

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

const EmitVersionedEventMarker = "[ApiEvents][emitRunEvent][BLOCK_EMIT_VERSIONED_EVENT]"

var (
	ErrInvalidEventState   = fmt.Errorf("invalid_event_state")
	ErrEventDispatchFailed = fmt.Errorf("event_dispatch_failed")
)

type Broadcaster interface {
	Broadcast(ctx context.Context, envelope RunEventEnvelope) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, envelope RunEventEnvelope) error
}

type Service struct {
	broadcaster Broadcaster
	dispatcher  Dispatcher
}

type RunEventEnvelope struct {
	EventID       string    `json:"event_id"`
	AnalysisRunID string    `json:"analysis_run_id"`
	EventType     string    `json:"event_type"`
	Version       int64     `json:"version"`
	Status        string    `json:"status,omitempty"`
	Payload       []byte    `json:"payload,omitempty"`
	EmittedAt     time.Time `json:"emitted_at"`
}

func NewService(_ *storage.Repository, broadcaster Broadcaster, dispatcher Dispatcher) (*Service, error) {
	return &Service{broadcaster: broadcaster, dispatcher: dispatcher}, nil
}

func (s *Service) EmitRunEvent(ctx context.Context, event storage.RunEventRecord) error {
	if strings.TrimSpace(event.AnalysisRunID) == "" || strings.TrimSpace(event.EventType) == "" || event.Version < 1 {
		return ErrInvalidEventState
	}
	envelope := RunEventEnvelope{
		EventID:       event.ID,
		AnalysisRunID: event.AnalysisRunID,
		EventType:     event.EventType,
		Version:       event.Version,
		Status:        event.Status,
		Payload:       event.PayloadJSON,
		EmittedAt:     event.CreatedAt,
	}
	if s.broadcaster != nil {
		if err := s.broadcaster.Broadcast(ctx, envelope); err != nil {
			return fmt.Errorf("%w: broadcast: %v", ErrEventDispatchFailed, err)
		}
	}
	if s.dispatcher != nil {
		if err := s.dispatcher.Dispatch(ctx, envelope); err != nil {
			return fmt.Errorf("%w: dispatch: %v", ErrEventDispatchFailed, err)
		}
	}
	return nil
}

type HTTPWebhookDispatcher struct{}

type WebhookOption func(*HTTPWebhookDispatcher)

func WithWebhookLogger(_ interface {
	Printf(format string, args ...any)
}) WebhookOption {
	return func(*HTTPWebhookDispatcher) {}
}

func NewHTTPWebhookDispatcher(_ *storage.Repository, opts ...WebhookOption) (*HTTPWebhookDispatcher, error) {
	dispatcher := &HTTPWebhookDispatcher{}
	for _, opt := range opts {
		opt(dispatcher)
	}
	return dispatcher, nil
}

func (d *HTTPWebhookDispatcher) Dispatch(context.Context, RunEventEnvelope) error {
	return nil
}

func (d *HTTPWebhookDispatcher) Run(ctx context.Context, interval time.Duration, _ int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func EventStreamHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if service == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
