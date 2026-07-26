package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/danila/media-analysis-platform/apps/api/internal/api"
)

func TestRetentionCycleReclaimsEveryLeaseClassBeforeSweeping(t *testing.T) {
	t.Parallel()
	target := &maintenanceTargetFake{}
	cfg := runtimeConfig{retentionBatchSize: 17, retentionClaimDuration: 45 * time.Second}
	var logs bytes.Buffer
	runRetentionCycle(context.Background(), target, cfg, log.New(&logs, "", 0))

	if target.metadataBatch != 17 || target.exportBatch != 17 || target.reconcileBatch != 17 {
		t.Fatalf("maintenance batches = metadata:%d export:%d reconcile:%d", target.metadataBatch, target.exportBatch, target.reconcileBatch)
	}
	if target.sweep.BatchSize != 17 || target.sweep.DeletionOwner != "api-retention" || target.sweep.ClaimSeconds != 45 {
		t.Fatalf("retention sweep request = %#v", target.sweep)
	}
	for _, anchor := range []string{"export_lease_reclaimed", "retention_reconciled_orphan", "stored_object_missing", "retention_claimed"} {
		if !strings.Contains(logs.String(), anchor) {
			t.Fatalf("maintenance log is missing %q: %s", anchor, logs.String())
		}
	}
}

type maintenanceTargetFake struct {
	api.TargetService
	metadataBatch  int
	exportBatch    int
	reconcileBatch int
	sweep          api.TargetRetentionSweepRequest
}

func (f *maintenanceTargetFake) ReclaimMetadataEnrichments(_ context.Context, batchSize int) (api.TargetMetadataEnrichmentReclaimResult, error) {
	f.metadataBatch = batchSize
	return api.TargetMetadataEnrichmentReclaimResult{}, nil
}

func (f *maintenanceTargetFake) ReclaimExportJobs(_ context.Context, req api.TargetExportReclaimRequest) (api.TargetExportReclaimResult, error) {
	f.exportBatch = req.BatchSize
	return api.TargetExportReclaimResult{Examined: 2, Requeued: 1, Failed: 1}, nil
}

func (f *maintenanceTargetFake) ReconcileRetention(_ context.Context, req api.TargetRetentionReconcileRequest) (api.TargetRetentionReconcileResult, error) {
	f.reconcileBatch = req.BatchSize
	return api.TargetRetentionReconcileResult{Examined: 3, OrphansDeleted: 1, ObjectsMarkedMissing: 1}, nil
}

func (f *maintenanceTargetFake) SweepRetention(_ context.Context, req api.TargetRetentionSweepRequest) (api.TargetRetentionSweepResult, error) {
	f.sweep = req
	return api.TargetRetentionSweepResult{Claimed: 1, Deleted: 1}, nil
}
