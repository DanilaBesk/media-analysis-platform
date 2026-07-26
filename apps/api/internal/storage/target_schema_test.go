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

func TestGovernedMediaExportRetentionMigrationIsAdditive(t *testing.T) {
	t.Parallel()
	migration := readStorageTextFixture(t, "migrations", "0002_governed_media_export_retention.sql")
	for _, fragment := range []string{
		"ADD COLUMN channel_account_id uuid",
		"generation_published_at",
		"CREATE UNIQUE INDEX stored_objects_channel_digest_unique_idx",
		"CREATE TABLE stored_object_pins",
		"CREATE TABLE export_jobs",
		"CREATE TABLE export_deliveries",
		"delete_lease_expires_at",
		"attempt_token",
		"selection_snapshot_items item",
		"0002 is forward-only",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("governed media migration missing %q", fragment)
		}
	}
	if strings.Contains(migration, "DROP TABLE stored_objects") {
		t.Fatal("governed media migration must not replace the stored_objects authority")
	}
	if strings.Contains(migration, "DROP TABLE IF EXISTS export_jobs") {
		t.Fatal("governed media migration rollback must preserve additive operational data")
	}
}

func TestYouTubeMetadataEnrichmentMigrationIsAdditiveAndFenced(t *testing.T) {
	t.Parallel()
	migration := readStorageTextFixture(t, "migrations", "0003_youtube_metadata_enrichment.sql")
	for _, fragment := range []string{
		"CREATE TABLE metadata_enrichment_jobs",
		"canonical_url text NOT NULL",
		"attempt_token text NULL",
		"lease_expires_at timestamptz NULL",
		"CREATE UNIQUE INDEX metadata_enrichment_asset_active_idx",
		"CREATE INDEX metadata_enrichment_lease_idx",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("metadata enrichment migration missing %q", fragment)
		}
	}
	if strings.Contains(migration, "ALTER TABLE selection_snapshot") {
		t.Fatal("metadata enrichment migration must not mutate immutable snapshot tables")
	}
}

func TestYouTubeMetadataBackfillCanonicalizesAndRepairsProviderURLs(t *testing.T) {
	t.Parallel()
	backfill := readStorageTextFixture(t, "migrations", "0004_backfill_youtube_metadata_enrichment.sql")
	for _, fragment := range []string{
		"[?&]v=([A-Za-z0-9_-]{11}",
		"^https://youtu\\.be/([A-Za-z0-9_-]{11}",
		"youtube\\.com/shorts/([A-Za-z0-9_-]{11}",
		"'https://www.youtube.com/watch?v=' || video_id",
		"WHERE video_id IS NOT NULL",
	} {
		if !strings.Contains(backfill, fragment) {
			t.Fatalf("metadata backfill migration missing %q", fragment)
		}
	}

	repair := readStorageTextFixture(t, "migrations", "0005_repair_youtube_metadata_backfill_urls.sql")
	for _, fragment := range []string{
		"SET canonical_url = 'https://www.youtube.com/watch?v=' || repairable.video_id",
		"youtube\\.com/shorts/([A-Za-z0-9_-]{11}",
		"error_code = 'provider_url_invalid'",
		"canonical_url !~ '^https://www\\.youtube\\.com/watch\\?v=[A-Za-z0-9_-]{11}$'",
	} {
		if !strings.Contains(repair, fragment) {
			t.Fatalf("metadata repair migration missing %q", fragment)
		}
	}

	shortsRepair := readStorageTextFixture(t, "migrations", "0006_requeue_youtube_shorts_backfill.sql")
	for _, fragment := range []string{
		"youtube\\.com/shorts/([A-Za-z0-9_-]{11}",
		"status = 'queued'",
		"error_code = NULL",
	} {
		if !strings.Contains(shortsRepair, fragment) {
			t.Fatalf("Shorts repair migration missing %q", fragment)
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

func TestGovernedMediaMigrationCanonicalizesSnapshotStorageLocator(t *testing.T) {
	t.Parallel()
	migration := readStorageTextFixture(t, "migrations", "0002_governed_media_export_retention.sql")
	for _, fragment := range []string{
		"'stored_object_id', aliases.canonical_stored_object_id::text",
		"'bucket', canonical.bucket",
		"'object_key', canonical.object_key",
		"JOIN stored_objects canonical ON canonical.id=aliases.canonical_stored_object_id",
	} {
		if !strings.Contains(migration, fragment) {
			t.Fatalf("governed-media migration missing snapshot locator rewrite %q", fragment)
		}
	}

	repair := readStorageTextFixture(t, "migrations", "0007_repair_snapshot_alias_locators.sql")
	if !strings.Contains(repair, "storage_snapshot=item.storage_snapshot || jsonb_build_object") {
		t.Fatal("snapshot alias repair must replace the complete durable locator")
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
