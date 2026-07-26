package api

import (
	"fmt"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

func NewRuntimeDependenciesWithTargetObjectStore(targetState TargetStateStore, targetObjects storage.ObjectStore, websocket WebsocketAcceptor, options ...TargetRuntimeOption) (Dependencies, error) {
	if targetState == nil {
		return Dependencies{}, fmt.Errorf("%w: target storage is required", storage.ErrContractViolation)
	}
	targetOptions := append([]TargetRuntimeOption{WithTargetObjectStore(targetObjects)}, options...)
	return Dependencies{
		Target:    NewTargetRuntimeService(targetState, targetOptions...),
		Websocket: websocket,
	}, nil
}

var _ TargetStateStore = (*targetstore.Store)(nil)
