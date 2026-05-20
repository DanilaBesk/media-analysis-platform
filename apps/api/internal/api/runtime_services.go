package api

import (
	"fmt"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

func NewRuntimeDependenciesWithTargetObjectStore(targetState TargetStateStore, targetObjects storage.ObjectStore, websocket WebsocketAcceptor) (Dependencies, error) {
	if targetState == nil {
		return Dependencies{}, fmt.Errorf("%w: target storage is required", storage.ErrContractViolation)
	}
	return Dependencies{
		Target:    NewTargetRuntimeService(targetState, WithTargetObjectStore(targetObjects)),
		Websocket: websocket,
	}, nil
}

var _ TargetStateStore = (*targetstore.Store)(nil)
