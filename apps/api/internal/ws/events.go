package ws

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
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

func NewService(broadcaster Broadcaster, dispatcher Dispatcher) (*Service, error) {
	return &Service{broadcaster: broadcaster, dispatcher: dispatcher}, nil
}

func (s *Service) EmitRunEvent(ctx context.Context, envelope RunEventEnvelope) error {
	if strings.TrimSpace(envelope.AnalysisRunID) == "" || strings.TrimSpace(envelope.EventType) == "" || envelope.Version < 1 {
		return ErrInvalidEventState
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

func EventStreamHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if service == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
