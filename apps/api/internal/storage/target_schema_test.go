package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTargetStorageMigrationDefinesChannelAwareSchema(t *testing.T) {
	t.Parallel()

	migration := readStorageTextFixture(t, "migrations", "0001_final_inbox_analysis_run_schema.sql")
	required := []string{
		"CREATE TABLE channel_accounts",
		"CREATE TABLE operation_requests",
		"CREATE TABLE stored_objects",
		"CREATE TABLE media_assets",
		"CREATE TABLE collections",
		"CREATE TABLE collection_items",
		"CREATE TABLE selection_snapshots",
		"CREATE TABLE selection_snapshot_items",
		"CREATE TABLE analysis_runs",
		"CREATE TABLE analysis_run_steps",
		"CREATE TABLE analysis_run_events",
		"CREATE TABLE artifacts",
		"CREATE TABLE analysis_run_step_inputs",
		"CREATE TABLE artifact_subjects",
		"CREATE TABLE diagnostics",
		"CREATE TABLE channel_surfaces",
		"CREATE TABLE channel_surface_subjects",
		"CREATE TABLE channel_surface_events",
		"CREATE UNIQUE INDEX operation_requests_channel_idempotency_unique_idx",
		"CREATE UNIQUE INDEX stored_objects_bucket_key_unique_idx",
		"CREATE UNIQUE INDEX collections_active_inbox_channel_unique_idx",
		"CREATE UNIQUE INDEX collection_items_active_asset_unique_idx",
		"CREATE UNIQUE INDEX selection_snapshot_items_position_unique_idx",
		"CREATE TRIGGER selection_snapshots_immutable_update_trg",
		"CREATE TRIGGER selection_snapshots_immutable_delete_trg",
		"CREATE UNIQUE INDEX channel_surfaces_active_key_idx",
		"CREATE UNIQUE INDEX channel_surfaces_active_address_idx",
		"safe_channel_context jsonb NOT NULL DEFAULT '{}'::jsonb",
	}
	for _, fragment := range required {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("target migration missing required fragment %q", fragment)
		}
	}
}

func TestTargetStorageMigrationDoesNotCreateLegacyStorageSurface(t *testing.T) {
	t.Parallel()

	migration := readStorageTextFixture(t, "migrations", "0001_final_inbox_analysis_run_schema.sql")
	forbidden := []string{
		"CREATE TABLE sources",
		"CREATE TABLE media_items",
		"CREATE TABLE selections",
		"CREATE TABLE selection_items",
		"CREATE TABLE analysis_run_tasks",
		"owner_type text",
		"owner_id text",
		"tenant_id text",
		"safe_adapter_context",
		"media_item_id",
		"selection_id uuid",
	}
	for _, fragment := range forbidden {
		if strings.Contains(migration, fragment) {
			t.Fatalf("target migration still contains legacy fragment %q", fragment)
		}
	}
}

func TestRequiredSchemaFragmentsTrackTargetStorageContract(t *testing.T) {
	t.Parallel()

	fragments := readStorageTextFixture(t, "testdata", "required_schema_fragments.txt")
	required := []string{
		"CREATE TABLE channel_accounts",
		"CREATE TABLE operation_requests",
		"CREATE TABLE stored_objects",
		"CREATE TABLE media_assets",
		"CREATE TABLE selection_snapshots",
		"CREATE TABLE analysis_run_steps",
		"CREATE TABLE analysis_run_step_inputs",
		"CREATE TABLE artifact_subjects",
		"CREATE TABLE channel_surfaces",
		"CREATE UNIQUE INDEX operation_requests_channel_idempotency_unique_idx",
		"CREATE UNIQUE INDEX channel_surfaces_active_key_idx",
		"CREATE TRIGGER selection_snapshots_immutable_update_trg",
	}
	for _, fragment := range required {
		if !strings.Contains(fragments, fragment) {
			t.Fatalf("required schema fragments missing target fragment %q", fragment)
		}
	}

	forbidden := []string{
		"CREATE TABLE sources",
		"CREATE TABLE media_items",
		"CREATE TABLE selections",
		"CREATE TABLE analysis_run_tasks",
		"owner_type text",
		"safe_adapter_context",
	}
	for _, fragment := range forbidden {
		if strings.Contains(fragments, fragment) {
			t.Fatalf("required schema fragments still contain legacy fragment %q", fragment)
		}
	}
}

func readStorageTextFixture(t *testing.T, elem ...string) string {
	t.Helper()

	path := filepath.Join(append([]string{""}, elem...)...)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}
