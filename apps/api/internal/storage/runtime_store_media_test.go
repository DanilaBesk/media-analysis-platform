package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLStateStoreAddMediaItemAndSoftDelete(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	deletedAt := now.Add(15 * time.Minute)
	size := int64(5)

	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
			},
			{
				match:   "SELECT id FROM collections",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"target-collection"}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"inbox-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows: [][]driver.Value{{
					"media-1", int64(0), "", now, nil,
				}},
			},
			{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				rows: [][]driver.Value{mediaItemDriverRow(mediaItemDriverRowInput{
					id:             "media-1",
					ownerType:      "telegram",
					ownerID:        "chat-1",
					sourceID:       "source-1",
					originType:     "object",
					objectKey:      "sources/source-1/source.ogg",
					checksum:       "sha256:abc",
					sizeBytes:      &size,
					mimeType:       "audio/ogg",
					kind:           "voice",
					status:         MediaStatusDeleted,
					displayName:    "voice.ogg",
					metadataJSON:   []byte(`{"lang":"ru"}`),
					retentionState: RetentionStateSoftDeleted,
					createdAt:      now,
					updatedAt:      deletedAt,
					deletedAt:      &deletedAt,
				})},
			},
		},
		execResponses: []scriptedExecResponse{
			{match: "INSERT INTO sources", affected: 1},
			{match: "INSERT INTO media_items", affected: 1},
			{match: "INSERT INTO collections", affected: 1},
			{match: "INSERT INTO collection_items", affected: 1},
			{match: "INSERT INTO collection_items", affected: 1},
			{match: "UPDATE media_items", affected: 1},
			{match: "UPDATE collection_items", affected: 2},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}

	item := MediaItemRecord{
		ID:    "media-1",
		Owner: OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"},
		Source: MediaSourceMetadata{
			SourceID:   "source-1",
			OriginType: "object",
			ObjectKey:  "sources/source-1/source.ogg",
			Checksum:   "sha256:abc",
			SizeBytes:  &size,
			MIMEType:   "audio/ogg",
		},
		Kind:        "voice",
		Status:      MediaStatusReady,
		DisplayName: "voice.ogg",
		MetadataJSON: []byte(
			`{"lang":"ru"}`,
		),
		Retention: RetentionMetadata{State: RetentionStateActive},
		CreatedAt: now,
		UpdatedAt: now,
	}
	inbox := CollectionRecord{
		ID:        "inbox-1",
		Owner:     item.Owner,
		Kind:      CollectionKindInbox,
		Name:      "Inbox",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	created, collection, err := store.AddMediaItem(context.Background(), item, inbox, "target-collection")
	if err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	if created.ID != item.ID {
		t.Fatalf("created id = %q, want %q", created.ID, item.ID)
	}
	if collection.ID != inbox.ID || len(collection.Items) != 1 || collection.Items[0].MediaItemID != item.ID {
		t.Fatalf("collection = %#v, want inbox membership for %q", collection, item.ID)
	}

	deleted, err := store.SoftDeleteMediaItem(context.Background(), item.Owner, item.ID, deletedAt)
	if err != nil {
		t.Fatalf("SoftDeleteMediaItem() error = %v", err)
	}
	if deleted.Status != MediaStatusDeleted {
		t.Fatalf("deleted status = %q, want %q", deleted.Status, MediaStatusDeleted)
	}
	if deleted.Retention.State != RetentionStateSoftDeleted {
		t.Fatalf("deleted retention = %q, want %q", deleted.Retention.State, RetentionStateSoftDeleted)
	}
	if deleted.DeletedAt == nil || !deleted.DeletedAt.Equal(deletedAt) {
		t.Fatalf("deleted_at = %#v, want %s", deleted.DeletedAt, deletedAt)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreAddMediaItemAppendsAfterExistingActiveCollectionItems(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 11, 20, 0, 0, time.UTC)
	size := int64(5)
	owner := OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"}

	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"inbox-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT id FROM collections",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"target-collection"}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"inbox-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows: [][]driver.Value{
					{"old-media", int64(0), "", now.Add(-time.Minute), nil},
					{"media-2", int64(1), "", now, nil},
				},
			},
		},
		execResponses: []scriptedExecResponse{
			{match: "INSERT INTO sources", affected: 1},
			{match: "INSERT INTO media_items", affected: 1},
			{match: "COALESCE(MAX(position) + 1, 0)", affected: 1, checkArgs: expectExecArgs(map[int]any{1: "inbox-1", 2: "media-2"})},
			{match: "COALESCE(MAX(position) + 1, 0)", affected: 1, checkArgs: expectExecArgs(map[int]any{1: "target-collection", 2: "media-2"})},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}

	item := MediaItemRecord{
		ID:    "media-2",
		Owner: owner,
		Source: MediaSourceMetadata{
			SourceID:   "source-2",
			OriginType: "object",
			ObjectKey:  "sources/source-2/source.ogg",
			Checksum:   "sha256:def",
			SizeBytes:  &size,
			MIMEType:   "audio/ogg",
		},
		Kind:         "voice",
		Status:       MediaStatusReady,
		DisplayName:  "voice-2.ogg",
		MetadataJSON: []byte(`{}`),
		Retention:    RetentionMetadata{State: RetentionStateActive},
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	inbox := CollectionRecord{
		ID:        "unused-new-inbox",
		Owner:     owner,
		Kind:      CollectionKindInbox,
		Name:      "Inbox",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	created, collection, err := store.AddMediaItem(context.Background(), item, inbox, "target-collection")
	if err != nil {
		t.Fatalf("AddMediaItem() error = %v", err)
	}
	if created.ID != item.ID {
		t.Fatalf("created id = %q, want %q", created.ID, item.ID)
	}
	if collection.ID != "inbox-1" || len(collection.Items) != 2 || collection.Items[1].Position != 1 {
		t.Fatalf("collection = %#v, want new inbox item appended at position 1", collection)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreGetCollectionReturnsEmptyItemsArray(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 12, 12, 15, 0, 0, time.UTC)
	owner := OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"}
	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"collection-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(5), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
			},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}

	collection, err := store.GetCollection(context.Background(), owner, "collection-1")
	if err != nil {
		t.Fatalf("GetCollection() error = %v", err)
	}
	if collection.Items == nil {
		t.Fatalf("collection items = nil, want empty array slice")
	}
	if len(collection.Items) != 0 {
		t.Fatalf("collection items = %#v, want empty", collection.Items)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreAddMediaItemAndSoftDeleteErrorMappings(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 12, 30, 0, 0, time.UTC)
	owner := OwnerScope{OwnerType: "telegram", OwnerID: "chat-1"}
	item := MediaItemRecord{
		ID:    "media-1",
		Owner: owner,
		Source: MediaSourceMetadata{
			SourceID:   "source-1",
			OriginType: "object",
			ObjectKey:  "sources/source-1/source.ogg",
			MIMEType:   "audio/ogg",
		},
		Kind:        "voice",
		Status:      MediaStatusReady,
		DisplayName: "voice.ogg",
		Retention:   RetentionMetadata{State: RetentionStateActive},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	inbox := CollectionRecord{
		ID:        "inbox-1",
		Owner:     owner,
		Kind:      CollectionKindInbox,
		Name:      "Inbox",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("add media item propagates target collection lookup failures", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
				},
				{
					match:   "SELECT id FROM collections",
					columns: []string{"id"},
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collections", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, _, err := store.AddMediaItem(context.Background(), item, inbox, "missing-target"); !errors.Is(err, ErrCollectionNotFound) {
			t.Fatalf("AddMediaItem(target missing) error = %v, want ErrCollectionNotFound", err)
		}
	})

	t.Run("add media item propagates membership insert errors", func(t *testing.T) {
		t.Parallel()

		stepErr := errors.New("membership failed")
		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collections", affected: 1},
				{match: "INSERT INTO collection_items", err: stepErr},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, _, err := store.AddMediaItem(context.Background(), item, inbox, ""); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(insert membership) error = %v, want stepErr", err)
		}
	})

	t.Run("add media item propagates target collection lookup query errors", func(t *testing.T) {
		t.Parallel()

		stepErr := errors.New("target collection lookup failed")
		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"inbox-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
					}},
				},
				{
					match:   "SELECT id FROM collections",
					columns: []string{"id"},
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, _, err := store.AddMediaItem(context.Background(), item, inbox, "target-collection"); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(target lookup query) error = %v, want stepErr", err)
		}
	})

	t.Run("add media item propagates early insert failures and ignores collection refresh errors", func(t *testing.T) {
		t.Parallel()

		stepErr := errors.New("early insert or refresh failed")

		storeSourceInsertErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "INSERT INTO sources",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(source insert) error = %v", err)
		}
		if _, _, err := storeSourceInsertErr.AddMediaItem(context.Background(), item, inbox, ""); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(source insert) error = %v, want stepErr", err)
		}

		storeMediaInsertErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(media insert) error = %v", err)
		}
		if _, _, err := storeMediaInsertErr.AddMediaItem(context.Background(), item, inbox, ""); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(media insert) error = %v, want stepErr", err)
		}

		storeRefreshErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"inbox-1", "telegram", "chat-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
					}},
				},
				{
					match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
					columns: collectionColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(refresh err) error = %v", err)
		}

		created, collection, err := storeRefreshErr.AddMediaItem(context.Background(), item, inbox, "")
		if err != nil {
			t.Fatalf("AddMediaItem(refresh ignored) error = %v", err)
		}
		if created.ID != item.ID {
			t.Fatalf("created = %#v, want original item", created)
		}
		if collection.ID != "" {
			t.Fatalf("collection = %#v, want zero-value collection when refresh lookup fails", collection)
		}
	})

	t.Run("soft delete maps not found and collection update failures", func(t *testing.T) {
		t.Parallel()

		notFoundStore, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items", affected: 0},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(not found) error = %v", err)
		}
		if _, err := notFoundStore.SoftDeleteMediaItem(context.Background(), owner, "missing-media", now); !errors.Is(err, ErrMediaItemNotFound) {
			t.Fatalf("SoftDeleteMediaItem(not found) error = %v, want ErrMediaItemNotFound", err)
		}

		stepErr := errors.New("remove memberships failed")
		failStore, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items", affected: 1},
				{match: "UPDATE collection_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(fail store) error = %v", err)
		}
		if _, err := failStore.SoftDeleteMediaItem(context.Background(), owner, "media-1", now); !errors.Is(err, stepErr) {
			t.Fatalf("SoftDeleteMediaItem(collection update) error = %v, want stepErr", err)
		}

		updateErrStore, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update err store) error = %v", err)
		}
		if _, err := updateErrStore.SoftDeleteMediaItem(context.Background(), owner, "media-1", now); !errors.Is(err, stepErr) {
			t.Fatalf("SoftDeleteMediaItem(media update) error = %v, want stepErr", err)
		}
	})
}

func TestSQLStateStoreReadQueriesAcrossCollectionsRunsAndArtifacts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 13, 0, 0, 0, time.UTC)
	size := int64(42)
	expiresAt := now.Add(2 * time.Hour)

	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				rows: [][]driver.Value{
					mediaItemDriverRow(mediaItemDriverRowInput{
						id:             "media-1",
						ownerType:      "web",
						ownerID:        "user-1",
						sourceID:       "source-1",
						originType:     "object",
						objectKey:      "sources/source-1/source.txt",
						checksum:       "sha256:111",
						sizeBytes:      &size,
						mimeType:       "text/plain",
						kind:           "text",
						status:         MediaStatusReady,
						displayName:    "source.txt",
						metadataJSON:   []byte(`{"tag":"one"}`),
						retentionState: RetentionStateActive,
						createdAt:      now,
						updatedAt:      now,
					}),
				},
			},
			{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				rows: [][]driver.Value{
					mediaItemDriverRow(mediaItemDriverRowInput{
						id:             "media-1",
						ownerType:      "web",
						ownerID:        "user-1",
						sourceID:       "source-1",
						originType:     "object",
						objectKey:      "sources/source-1/source.txt",
						checksum:       "sha256:111",
						sizeBytes:      &size,
						mimeType:       "text/plain",
						kind:           "text",
						status:         MediaStatusReady,
						displayName:    "source.txt",
						metadataJSON:   []byte(`{"tag":"one"}`),
						retentionState: RetentionStateActive,
						createdAt:      now,
						updatedAt:      now,
					}),
				},
			},
			{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
			},
			{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"collection-1", "web", "user-1", "", CollectionKindUser, "Review", CollectionStatusActive, int64(3), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows: [][]driver.Value{{
					"media-1", int64(0), "tester", now, nil,
				}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows: [][]driver.Value{{
					"collection-1", "web", "user-1", "", CollectionKindUser, "Review", CollectionStatusActive, int64(3), now, now, nil, nil,
				}},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows: [][]driver.Value{{
					"media-1", int64(0), "tester", now, nil,
				}},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE owner_type=$1 AND owner_id=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "SELECT id, analysis_run_id, event_type, version, payload, COALESCE(status,''), created_at FROM analysis_run_events",
				columns: runEventColumns(),
				rows: [][]driver.Value{{
					"event-1", "run-1", "analysis_run.created", int64(1), []byte(`{"stage":"created"}`), AnalysisRunStatusQueued, now,
				}},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows: [][]driver.Value{{
					"selection-1", "web", "user-1", "", SelectionStatusSealed, "collection-1", []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now,
				}},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows:    [][]driver.Value{artifactDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows:    [][]driver.Value{artifactDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows:    [][]driver.Value{artifactDriverRow(now, expiresAt)},
			},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	items, err := store.ListMediaItems(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListMediaItems() error = %v", err)
	}
	if len(items) != 1 || items[0].ID != "media-1" {
		t.Fatalf("items = %#v, want one media item", items)
	}

	item, err := store.GetMediaItem(context.Background(), owner, "media-1")
	if err != nil {
		t.Fatalf("GetMediaItem() error = %v", err)
	}
	if item.DisplayName != "source.txt" {
		t.Fatalf("display name = %q, want source.txt", item.DisplayName)
	}
	if _, err := store.GetMediaItem(context.Background(), owner, "missing"); !errors.Is(err, ErrMediaItemNotFound) {
		t.Fatalf("GetMediaItem(missing) error = %v, want ErrMediaItemNotFound", err)
	}

	collections, err := store.ListCollections(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListCollections() error = %v", err)
	}
	if len(collections) != 1 || len(collections[0].Items) != 1 {
		t.Fatalf("collections = %#v, want one collection with one item", collections)
	}

	collection, err := store.GetCollection(context.Background(), owner, "collection-1")
	if err != nil {
		t.Fatalf("GetCollection() error = %v", err)
	}
	if collection.Version != 3 || len(collection.Items) != 1 {
		t.Fatalf("collection = %#v, want version 3 with one item", collection)
	}

	selection, err := store.GetSelection(context.Background(), owner, "selection-1")
	if err != nil {
		t.Fatalf("GetSelection() error = %v", err)
	}
	if len(selection.Items) != 1 || selection.Items[0].DisplayName != "source.txt" {
		t.Fatalf("selection = %#v, want one sealed snapshot item", selection)
	}

	runByID, err := store.GetAnalysisRunByID(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("GetAnalysisRunByID() error = %v", err)
	}
	if runByID.Selection.ID != "selection-1" || runByID.Status != AnalysisRunStatusQueued {
		t.Fatalf("runByID = %#v", runByID)
	}

	runByOwner, err := store.GetAnalysisRun(context.Background(), owner, "run-1")
	if err != nil {
		t.Fatalf("GetAnalysisRun() error = %v", err)
	}
	if runByOwner.ID != "run-1" || runByOwner.Selection.ID != "selection-1" {
		t.Fatalf("runByOwner = %#v", runByOwner)
	}

	runs, err := store.ListAnalysisRuns(context.Background(), owner)
	if err != nil {
		t.Fatalf("ListAnalysisRuns() error = %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "run-1" {
		t.Fatalf("runs = %#v, want one run", runs)
	}

	events, err := store.ListRunEvents(context.Background(), owner, "run-1")
	if err != nil {
		t.Fatalf("ListRunEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].EventType != "analysis_run.created" {
		t.Fatalf("events = %#v, want created event", events)
	}

	artifacts, err := store.ListArtifacts(context.Background(), owner, "run-1")
	if err != nil {
		t.Fatalf("ListArtifacts() error = %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].ID != "artifact-1" {
		t.Fatalf("artifacts = %#v, want one artifact", artifacts)
	}

	artifact, err := store.GetArtifact(context.Background(), owner, "artifact-1")
	if err != nil {
		t.Fatalf("GetArtifact() error = %v", err)
	}
	if artifact.AnalysisRunID != "run-1" || artifact.ObjectKey == "" {
		t.Fatalf("artifact = %#v", artifact)
	}

	artifactByID, err := store.GetArtifactByID(context.Background(), "artifact-1")
	if err != nil {
		t.Fatalf("GetArtifactByID() error = %v", err)
	}
	if artifactByID.ID != "artifact-1" {
		t.Fatalf("artifactByID = %#v", artifactByID)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreGetterNotFoundMappings(t *testing.T) {
	t.Parallel()

	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
			},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	if _, err := store.GetCollection(context.Background(), owner, "missing-collection"); !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("GetCollection(missing) error = %v, want ErrCollectionNotFound", err)
	}
	if _, err := store.GetSelection(context.Background(), owner, "missing-selection"); !errors.Is(err, ErrSelectionNotFound) {
		t.Fatalf("GetSelection(missing) error = %v, want ErrSelectionNotFound", err)
	}
	if _, err := store.GetAnalysisRunByID(context.Background(), "missing-run"); !errors.Is(err, ErrAnalysisRunNotFound) {
		t.Fatalf("GetAnalysisRunByID(missing) error = %v, want ErrAnalysisRunNotFound", err)
	}
	if _, err := store.GetAnalysisRun(context.Background(), owner, "missing-run"); !errors.Is(err, ErrAnalysisRunNotFound) {
		t.Fatalf("GetAnalysisRun(missing) error = %v, want ErrAnalysisRunNotFound", err)
	}
	if _, err := store.GetArtifact(context.Background(), owner, "missing-artifact"); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("GetArtifact(missing) error = %v, want ErrArtifactNotFound", err)
	}
	if _, err := store.GetArtifactByID(context.Background(), "missing-artifact"); !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("GetArtifactByID(missing) error = %v, want ErrArtifactNotFound", err)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreMutationNoRowMappings(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 13, 15, 0, 0, time.UTC)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	t.Run("create collection returns media item not found", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM media_items",
					columns: []string{"id"},
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO collections", affected: 1},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, err := store.CreateCollection(context.Background(), CollectionRecord{
			ID:        "collection-missing-item",
			Owner:     owner,
			Kind:      CollectionKindUser,
			Name:      "Needs existing media",
			Status:    CollectionStatusActive,
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}, []string{"missing-media"}); !errors.Is(err, ErrMediaItemNotFound) {
			t.Fatalf("CreateCollection(missing media) error = %v, want ErrMediaItemNotFound", err)
		}

		config.assertExhausted(t)
	})

	t.Run("update collection returns version conflict on no rows", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections", affected: 0},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, err := store.UpdateCollection(context.Background(), UpdateCollectionRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 7,
			Name:            "stale",
		}, now); !errors.Is(err, ErrCollectionVersionConflict) {
			t.Fatalf("UpdateCollection(stale) error = %v, want ErrCollectionVersionConflict", err)
		}

		config.assertExhausted(t)
	})

	t.Run("update collection items returns version conflict on no rows", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections SET version=version+1", affected: 0},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, err := store.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
		}, now); !errors.Is(err, ErrCollectionVersionConflict) {
			t.Fatalf("UpdateCollectionItems(stale) error = %v, want ErrCollectionVersionConflict", err)
		}

		config.assertExhausted(t)
	})

	t.Run("update collection items returns media item not found", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM media_items",
					columns: []string{"id"},
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections SET version=version+1", affected: 1},
				{match: "UPDATE collection_items SET removed_at=$1", affected: 1},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, err := store.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "missing-media", Position: 0}},
			AddedBy:         "tester",
		}, now); !errors.Is(err, ErrMediaItemNotFound) {
			t.Fatalf("UpdateCollectionItems(missing media) error = %v, want ErrMediaItemNotFound", err)
		}

		config.assertExhausted(t)
	})
}

func TestRuntimeStoreMediaAndCollectionErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 13, 20, 0, 0, time.UTC)
	size := int64(5)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("media-collection branch failed")

	item := MediaItemRecord{
		ID:    "media-1",
		Owner: owner,
		Source: MediaSourceMetadata{
			SourceID:   "source-1",
			OriginType: "object",
			ObjectKey:  "sources/source-1/source.ogg",
			Checksum:   "sha256:abc",
			SizeBytes:  &size,
			MIMEType:   "audio/ogg",
		},
		Kind:        "voice",
		Status:      MediaStatusReady,
		DisplayName: "voice.ogg",
		MetadataJSON: []byte(
			`{"lang":"ru"}`,
		),
		Retention: RetentionMetadata{State: RetentionStateActive},
		CreatedAt: now,
		UpdatedAt: now,
	}
	inbox := CollectionRecord{
		ID:        "inbox-1",
		Owner:     owner,
		Kind:      CollectionKindInbox,
		Name:      "Inbox",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	t.Run("add media item propagates inbox lookup, inbox insert, and target membership errors", func(t *testing.T) {
		t.Parallel()

		storeLookupErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
				err:     stepErr,
			}},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(lookup) error = %v", err)
		}
		if _, _, err := storeLookupErr.AddMediaItem(context.Background(), item, inbox, ""); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(inbox lookup) error = %v, want stepErr", err)
		}

		storeInboxInsertErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
			}},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collections", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(inbox insert) error = %v", err)
		}
		if _, _, err := storeInboxInsertErr.AddMediaItem(context.Background(), item, inbox, ""); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(inbox insert) error = %v, want stepErr", err)
		}

		storeTargetMembershipErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"inbox-1", "web", "user-1", "", CollectionKindInbox, "Inbox", CollectionStatusActive, int64(1), now, now, nil, nil,
					}},
				},
				{
					match:   "SELECT id FROM collections",
					columns: []string{"id"},
					rows:    [][]driver.Value{{"target-collection"}},
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO sources", affected: 1},
				{match: "INSERT INTO media_items", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
				{match: "INSERT INTO collection_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(target membership) error = %v", err)
		}
		if _, _, err := storeTargetMembershipErr.AddMediaItem(context.Background(), item, inbox, "target-collection"); !errors.Is(err, stepErr) {
			t.Fatalf("AddMediaItem(target membership) error = %v, want stepErr", err)
		}
	})

	t.Run("list and get media items propagate query and scan errors", func(t *testing.T) {
		t.Parallel()

		storeListQueryErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(list query) error = %v", err)
		}
		if _, err := storeListQueryErr.ListMediaItems(context.Background(), owner); !errors.Is(err, stepErr) {
			t.Fatalf("ListMediaItems(query) error = %v, want stepErr", err)
		}

		storeListScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				rows: [][]driver.Value{{
					"media-1", "web", "user-1", "", "source-1", "object", "", "sources/source-1/source.ogg", "", "", "bad-size", "audio/ogg", nil, "voice", MediaStatusReady, "voice.ogg", "", []byte(`{"lang":"ru"}`), RetentionStateActive, "", nil, nil, now, now,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(list scan) error = %v", err)
		}
		if _, err := storeListScanErr.ListMediaItems(context.Background(), owner); err == nil {
			t.Fatalf("ListMediaItems(scan) error = nil, want scan failure")
		}

		storeGetScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(get scan) error = %v", err)
		}
		if _, err := storeGetScanErr.GetMediaItem(context.Background(), owner, "media-1"); !errors.Is(err, stepErr) {
			t.Fatalf("GetMediaItem(scan) error = %v, want stepErr", err)
		}
	})

	t.Run("create and read collections cover refresh and item-query branches", func(t *testing.T) {
		t.Parallel()

		storeListCollectionsQueryErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(list collections query) error = %v", err)
		}
		if _, err := storeListCollectionsQueryErr.ListCollections(context.Background(), owner); !errors.Is(err, stepErr) {
			t.Fatalf("ListCollections(query) error = %v, want stepErr", err)
		}

		storeListCollectionsScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"collection-1", "web", "user-1", "", CollectionKindUser, "Review", CollectionStatusActive, "bad-version", now, now, nil, nil,
					}},
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(list collections scan) error = %v", err)
		}
		if _, err := storeListCollectionsScanErr.ListCollections(context.Background(), owner); err == nil {
			t.Fatalf("ListCollections(scan) error = nil, want scan failure")
		}

		storeCreateInsertErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "INSERT INTO collections",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(create insert) error = %v", err)
		}
		if _, err := storeCreateInsertErr.CreateCollection(context.Background(), CollectionRecord{
			ID:        "collection-1",
			Owner:     owner,
			Kind:      CollectionKindUser,
			Name:      "Review",
			Status:    CollectionStatusActive,
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}, nil); !errors.Is(err, stepErr) {
			t.Fatalf("CreateCollection(insert) error = %v, want stepErr", err)
		}

		storeCreateMembershipErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "SELECT id FROM media_items",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"media-1"}},
			}},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO collections", affected: 1},
				{match: "INSERT INTO collection_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(create membership) error = %v", err)
		}
		if _, err := storeCreateMembershipErr.CreateCollection(context.Background(), CollectionRecord{
			ID:        "collection-1",
			Owner:     owner,
			Kind:      CollectionKindUser,
			Name:      "Review",
			Status:    CollectionStatusActive,
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}, []string{"media-1"}); !errors.Is(err, stepErr) {
			t.Fatalf("CreateCollection(membership) error = %v, want stepErr", err)
		}

		storeCreateRefreshErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM media_items",
					columns: []string{"id"},
					rows:    [][]driver.Value{{"media-1"}},
				},
				{
					match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
					columns: collectionColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO collections", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(create refresh) error = %v", err)
		}
		if _, err := storeCreateRefreshErr.CreateCollection(context.Background(), CollectionRecord{
			ID:        "collection-1",
			Owner:     owner,
			Kind:      CollectionKindUser,
			Name:      "Review",
			Status:    CollectionStatusActive,
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}, []string{"media-1"}); !errors.Is(err, stepErr) {
			t.Fatalf("CreateCollection(refresh) error = %v, want stepErr", err)
		}

		storeListCollectionsItemErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"collection-1", "web", "user-1", "", CollectionKindUser, "Review", CollectionStatusActive, int64(2), now, now, nil, nil,
					}},
				},
				{
					match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
					columns: collectionItemColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(list collections) error = %v", err)
		}
		collections, err := storeListCollectionsItemErr.ListCollections(context.Background(), owner)
		if err != nil {
			t.Fatalf("ListCollections() error = %v, want nil with ignored item lookup failure", err)
		}
		if len(collections) != 1 || len(collections[0].Items) != 0 {
			t.Fatalf("collections = %#v, want collection with skipped items on lookup failure", collections)
		}

		storeGetCollectionItemErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
					columns: collectionColumns(),
					rows: [][]driver.Value{{
						"collection-1", "web", "user-1", "", CollectionKindUser, "Review", CollectionStatusActive, int64(2), now, now, nil, nil,
					}},
				},
				{
					match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
					columns: collectionItemColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(get collection) error = %v", err)
		}
		if _, err := storeGetCollectionItemErr.GetCollection(context.Background(), owner, "collection-1"); !errors.Is(err, stepErr) {
			t.Fatalf("GetCollection(items) error = %v, want stepErr", err)
		}
	})

	t.Run("update collection and items propagate exec and refresh failures", func(t *testing.T) {
		t.Parallel()

		storeUpdateExecErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "UPDATE collections",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update exec) error = %v", err)
		}
		if _, err := storeUpdateExecErr.UpdateCollection(context.Background(), UpdateCollectionRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 2,
			Name:            "Renamed",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollection(exec) error = %v, want stepErr", err)
		}

		storeUpdateRefreshErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				err:     stepErr,
			}},
			execResponses: []scriptedExecResponse{{
				match:    "UPDATE collections",
				affected: 1,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update refresh) error = %v", err)
		}
		if _, err := storeUpdateRefreshErr.UpdateCollection(context.Background(), UpdateCollectionRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 2,
			Name:            "Renamed",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollection(refresh) error = %v, want stepErr", err)
		}

		storeUpdateItemsVersionExecErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "UPDATE collections SET version=version+1",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update items version exec) error = %v", err)
		}
		if _, err := storeUpdateItemsVersionExecErr.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
			AddedBy:         "tester",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollectionItems(version exec) error = %v, want stepErr", err)
		}

		storeUpdateItemsRemoveErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections SET version=version+1", affected: 1},
				{match: "UPDATE collection_items SET removed_at=$1", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update items remove) error = %v", err)
		}
		if _, err := storeUpdateItemsRemoveErr.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
			AddedBy:         "tester",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollectionItems(remove) error = %v, want stepErr", err)
		}

		storeUpdateItemsInsertErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "SELECT id FROM media_items",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"media-1"}},
			}},
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections SET version=version+1", affected: 1},
				{match: "UPDATE collection_items SET removed_at=$1", affected: 1},
				{match: "INSERT INTO collection_items", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update items insert) error = %v", err)
		}
		if _, err := storeUpdateItemsInsertErr.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
			AddedBy:         "tester",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollectionItems(insert) error = %v, want stepErr", err)
		}

		storeUpdateItemsRefreshErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM media_items",
					columns: []string{"id"},
					rows:    [][]driver.Value{{"media-1"}},
				},
				{
					match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
					columns: collectionColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "UPDATE collections SET version=version+1", affected: 1},
				{match: "UPDATE collection_items SET removed_at=$1", affected: 1},
				{match: "INSERT INTO collection_items", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(update items refresh) error = %v", err)
		}
		if _, err := storeUpdateItemsRefreshErr.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
			CollectionID:    "collection-1",
			Owner:           owner,
			ExpectedVersion: 4,
			Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}},
			AddedBy:         "tester",
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("UpdateCollectionItems(refresh) error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreSelectionHelpersNoRowMappings(t *testing.T) {
	t.Parallel()

	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	t.Run("selectMediaItemHeader returns media item not found", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM media_items",
					columns: []string{"id"},
				},
			},
		}

		db := openScriptedRuntimeStoreDB(t, config)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("BeginTx() error = %v", err)
		}
		defer tx.Rollback()

		if _, err := selectMediaItemHeader(context.Background(), tx, owner, "missing-media"); !errors.Is(err, ErrMediaItemNotFound) {
			t.Fatalf("selectMediaItemHeader(missing) error = %v, want ErrMediaItemNotFound", err)
		}

		config.assertExhausted(t)
	})

	t.Run("selectCollectionHeader returns collection not found", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "SELECT id FROM collections",
					columns: []string{"id"},
				},
			},
		}

		db := openScriptedRuntimeStoreDB(t, config)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("BeginTx() error = %v", err)
		}
		defer tx.Rollback()

		if _, err := selectCollectionHeader(context.Background(), tx, owner, "missing-collection"); !errors.Is(err, ErrCollectionNotFound) {
			t.Fatalf("selectCollectionHeader(missing) error = %v, want ErrCollectionNotFound", err)
		}

		config.assertExhausted(t)
	})

	t.Run("selectInboxCollection returns collection not found", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
					columns: collectionColumns(),
				},
			},
		}

		db := openScriptedRuntimeStoreDB(t, config)
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("BeginTx() error = %v", err)
		}
		defer tx.Rollback()

		if _, err := selectInboxCollection(context.Background(), tx, owner); !errors.Is(err, ErrCollectionNotFound) {
			t.Fatalf("selectInboxCollection(missing) error = %v, want ErrCollectionNotFound", err)
		}

		config.assertExhausted(t)
	})
}

func TestSQLStateStoreRunTransitionIdempotentBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 13, 30, 0, 0, time.UTC)
	startedAt := now.Add(-5 * time.Minute)
	completedAt := now.Add(10 * time.Minute)
	expiresAt := now.Add(2 * time.Hour)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	t.Run("finalize returns existing terminal run for same owner", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_runs\nSET status=$5",
					columns: []string{"version"},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusSucceeded, 4, &startedAt, &completedAt, nil)},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		run, err := store.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-terminal",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			PayloadJSON:   []byte(`{"result":"already_done"}`),
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     completedAt,
		}, completedAt)
		if err != nil {
			t.Fatalf("FinalizeAnalysisRunTask(idempotent) error = %v", err)
		}
		if run.Status != AnalysisRunStatusSucceeded || run.Version != 4 || run.Selection.ID != "selection-1" {
			t.Fatalf("run = %#v", run)
		}

		config.assertExhausted(t)
	})

	t.Run("finalize returns owner mismatch for foreign terminal run", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_runs\nSET status=$5",
					columns: []string{"version"},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					rows: [][]driver.Value{{
						"run-1", "telegram", "chat-9", "", "selection-1", "transcription", AnalysisRunStatusSucceeded, int64(4),
						[]byte(`{"language":"ru"}`), []byte(`{"strategy":"polling"}`), "not_required", now, startedAt, completedAt, nil, expiresAt,
					}},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, err := store.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-foreign",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			PayloadJSON:   []byte(`{"result":"foreign"}`),
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     completedAt,
		}, completedAt); !errors.Is(err, ErrOwnerMismatch) {
			t.Fatalf("FinalizeAnalysisRunTask(foreign) error = %v, want ErrOwnerMismatch", err)
		}

		config.assertExhausted(t)
	})

	t.Run("claim returns existing run without claim when no queue row updated", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
					columns: analysisRunColumns(),
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 7, &startedAt, nil, nil)},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		run, claimed, err := store.ClaimAnalysisRunTask(context.Background(), "run-1", "transcription", "selection.transcription", "worker-1", startedAt)
		if err != nil {
			t.Fatalf("ClaimAnalysisRunTask(idempotent) error = %v", err)
		}
		if claimed {
			t.Fatalf("claimed = true, want false")
		}
		if run.Status != AnalysisRunStatusRunning || run.Version != 7 || run.Selection.ID != "selection-1" {
			t.Fatalf("run = %#v", run)
		}

		config.assertExhausted(t)
	})
}

func TestSQLStateStoreOperationalQueriesAndCleanup(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 14, 0, 0, 0, time.UTC)

	config := &scriptedRuntimeStoreConfig{
		queryResponses: []scriptedQueryResponse{
			{
				match:   "FROM diagnostics\nWHERE owner_type=$1",
				columns: diagnosticColumns(),
				rows: [][]driver.Value{{
					"diag-1", "web", "user-1", "", "analysis_run", "run-1", "warning", "worker_partial", "partial success", []byte(`{"step":1}`), []byte(`{"safe":true}`), "corr-1", "retry", now,
				}},
			},
			{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunTaskColumns(),
				rows: [][]driver.Value{{
					"task-1", "run-1", "transcription", "selection.transcription", AnalysisRunTaskStatusPendingEnqueue, int64(1), "", nil, nil, nil, now,
				}},
			},
			{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunQueueColumns(),
				rows: [][]driver.Value{{
					"run-1", "transcription", "transcription", "selection.transcription", AnalysisRunTaskStatusQueued, int64(2), int64(1), now,
				}},
			},
			{
				match:   "FROM diagnostics\nWHERE code IN",
				columns: diagnosticColumns(),
				rows: [][]driver.Value{{
					"diag-ops", "system", "cleanup", "", "artifact", "artifact-1", "error", "orphan_object_cleanup_failed", "delete failed", []byte(`{"bucket":"artifacts"}`), []byte(`{}`), "", "", now,
				}},
			},
			{
				match:   "SELECT 'source', s.id::text",
				columns: orphanColumns(),
				rows: [][]driver.Value{{
					"source", "source-1", "web", "user-1", "", "sources", "sources/source-1/source.txt", "deleted_media_source",
				}, {
					"artifact", "artifact-1", "web", "user-1", "", "artifacts", "artifacts/run-1/report.md", "expired_artifact",
				}},
			},
		},
		execResponses: []scriptedExecResponse{
			{match: "UPDATE analysis_run_tasks\nSET status='queued'", affected: 1},
			{match: "UPDATE analysis_run_tasks\nSET status='queued'", affected: 0},
			{match: "UPDATE media_items mi", affected: 2},
			{match: "UPDATE collection_items ci", affected: 3},
			{match: "UPDATE collections c", affected: 1},
			{match: "UPDATE selections s", affected: 1},
			{match: "UPDATE analysis_runs\nSET status='expired'", affected: 1},
			{match: "UPDATE artifacts\nSET status='expired'", affected: 2},
			{match: "UPDATE media_items\nSET retention_state=$1", affected: 1},
			{match: "INSERT INTO diagnostics", affected: 1},
			{match: "UPDATE artifacts\nSET status=CASE WHEN $1 THEN 'deleted' ELSE status END", affected: 1},
			{match: "INSERT INTO diagnostics", affected: 1},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	diagnostics, err := store.ListDiagnostics(context.Background(), owner, DiagnosticQuery{Severity: "warning"})
	if err != nil {
		t.Fatalf("ListDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0].Code != "worker_partial" {
		t.Fatalf("diagnostics = %#v, want worker_partial", diagnostics)
	}

	pending, err := store.ListPendingEnqueueTasks(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListPendingEnqueueTasks() error = %v", err)
	}
	if len(pending) != 1 || pending[0].Status != AnalysisRunTaskStatusPendingEnqueue {
		t.Fatalf("pending = %#v, want one pending task", pending)
	}

	queue, err := store.ListAnalysisRunQueue(context.Background(), AnalysisRunTaskStatusQueued, "transcription", "selection.transcription", 5)
	if err != nil {
		t.Fatalf("ListAnalysisRunQueue() error = %v", err)
	}
	if len(queue) != 1 || queue[0].Status != AnalysisRunTaskStatusQueued {
		t.Fatalf("queue = %#v, want one queued task", queue)
	}

	if err := store.MarkAnalysisRunTaskQueued(context.Background(), "run-1", "selection.transcription", now); err != nil {
		t.Fatalf("MarkAnalysisRunTaskQueued() error = %v", err)
	}
	if err := store.MarkAnalysisRunTaskQueued(context.Background(), "run-1", "selection.transcription", now); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("MarkAnalysisRunTaskQueued(missing) error = %v, want ErrExecutionNotFound", err)
	}

	opsDiagnostics, err := store.ListOperationalDiagnostics(context.Background(), []string{"", "orphan_object_cleanup_failed"})
	if err != nil {
		t.Fatalf("ListOperationalDiagnostics() error = %v", err)
	}
	if len(opsDiagnostics) != 1 || opsDiagnostics[0].Code != "orphan_object_cleanup_failed" {
		t.Fatalf("ops diagnostics = %#v", opsDiagnostics)
	}
	emptyOpsDiagnostics, err := store.ListOperationalDiagnostics(context.Background(), []string{" ", ""})
	if err != nil {
		t.Fatalf("ListOperationalDiagnostics(empty) error = %v", err)
	}
	if len(emptyOpsDiagnostics) != 0 {
		t.Fatalf("empty ops diagnostics = %#v, want empty", emptyOpsDiagnostics)
	}

	retention, err := store.ApplyRetentionPolicies(context.Background(), now)
	if err != nil {
		t.Fatalf("ApplyRetentionPolicies() error = %v", err)
	}
	if retention.ExpiredMediaItems != 2 || retention.ExpiredArtifacts != 2 || retention.ArchivedCollections != 1 {
		t.Fatalf("retention = %#v", retention)
	}

	orphans, err := store.DetectOrphanObjects(context.Background())
	if err != nil {
		t.Fatalf("DetectOrphanObjects() error = %v", err)
	}
	if len(orphans) != 2 || orphans[0].Bucket != "sources" || orphans[1].Bucket != "artifacts" {
		t.Fatalf("orphans = %#v", orphans)
	}

	if err := store.RecordOrphanObjectCleanup(context.Background(), orphans[0], false, "metadata only", now); err != nil {
		t.Fatalf("RecordOrphanObjectCleanup(source) error = %v", err)
	}
	if err := store.RecordOrphanObjectCleanup(context.Background(), orphans[1], true, "delete failed at provider", now); err != nil {
		t.Fatalf("RecordOrphanObjectCleanup(artifact) error = %v", err)
	}
	if err := store.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{SubjectType: "unknown"}, false, "noop", now); !errors.Is(err, ErrContractViolation) {
		t.Fatalf("RecordOrphanObjectCleanup(unknown) error = %v, want ErrContractViolation", err)
	}

	if rowsAffectedInt(scriptedRowsAffected(7)) != 7 {
		t.Fatalf("rowsAffectedInt() must return RowsAffected")
	}
	if got := firstNonZeroTime(time.Time{}, now); !got.Equal(now) {
		t.Fatalf("firstNonZeroTime(zero, fallback) = %s, want %s", got, now)
	}
	if got := firstNonZeroTime(expiresAt(now), now); !got.Equal(expiresAt(now)) {
		t.Fatalf("firstNonZeroTime(candidate, fallback) = %s, want candidate", got)
	}
	if !strings.Contains(mediaItemSelectSQL(), "FROM media_items mi") {
		t.Fatalf("mediaItemSelectSQL() missing media_items join")
	}
	if !strings.Contains(artifactSelectSQL(), "FROM artifacts a") {
		t.Fatalf("artifactSelectSQL() missing artifacts table")
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreMutationAndExecutionLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 15, 0, 0, 0, time.UTC)
	startedAt := now.Add(10 * time.Minute)
	completedAt := now.Add(20 * time.Minute)
	expiresAt := now.Add(2 * time.Hour)
	size := int64(42)

	config := &scriptedRuntimeStoreConfig{
		execResponses: []scriptedExecResponse{
			{match: "INSERT INTO collections", affected: 1},
			{match: "INSERT INTO collection_items", affected: 1},
			{match: "UPDATE collections\nSET name=COALESCE", affected: 1},
			{match: "UPDATE collections SET version=version+1", affected: 1},
			{match: "UPDATE collection_items SET removed_at=$1", affected: 1},
			{match: "INSERT INTO collection_items", affected: 1},
			{match: "INSERT INTO selections", affected: 1},
			{match: "INSERT INTO selection_items", affected: 1},
			{match: "INSERT INTO analysis_runs", affected: 1},
			{match: "INSERT INTO analysis_run_tasks", affected: 1},
			{match: "INSERT INTO analysis_run_events", affected: 1},
			{match: "UPDATE analysis_runs\nSET status='running'", affected: 1},
			{match: "INSERT INTO analysis_run_events", affected: 1},
			{match: "UPDATE analysis_run_tasks\nSET heartbeat_at=$2", affected: 1},
			{match: "INSERT INTO artifacts", affected: 1},
			{match: "INSERT INTO diagnostics", affected: 1},
			{match: "UPDATE analysis_run_tasks\nSET status=$2, finalized_at=$3, heartbeat_at=$3", affected: 1},
			{match: "INSERT INTO analysis_run_events", affected: 1},
		},
		queryResponses: []scriptedQueryResponse{
			{
				match:   "SELECT id FROM media_items",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"media-1"}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows:    [][]driver.Value{collectionDriverRow("collection-1", "Review", CollectionStatusActive, 1, now)},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows:    [][]driver.Value{{"media-1", int64(0), "", now, nil}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows:    [][]driver.Value{collectionDriverRow("collection-1", "Review v2", CollectionStatusArchived, 2, now)},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows:    [][]driver.Value{{"media-1", int64(0), "", now, nil}},
			},
			{
				match:   "SELECT id FROM media_items",
				columns: []string{"id"},
				rows:    [][]driver.Value{{"media-1"}},
			},
			{
				match:   "FROM collections\nWHERE id=$1 AND owner_type=$2",
				columns: collectionColumns(),
				rows:    [][]driver.Value{collectionDriverRow("collection-1", "Review v2", CollectionStatusArchived, 3, now)},
			},
			{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows:    [][]driver.Value{{"media-1", int64(1), "tester", completedAt, nil}},
			},
			{
				match:   "FROM media_items mi",
				columns: mediaItemColumns(),
				rows: [][]driver.Value{mediaItemDriverRow(mediaItemDriverRowInput{
					id:             "media-1",
					ownerType:      "web",
					ownerID:        "user-1",
					sourceID:       "source-1",
					originType:     "object",
					objectKey:      "sources/source-1/source.txt",
					checksum:       "sha256:111",
					sizeBytes:      &size,
					mimeType:       "text/plain",
					kind:           "text",
					status:         MediaStatusReady,
					displayName:    "source.txt",
					metadataJSON:   []byte(`{"tag":"one"}`),
					retentionState: RetentionStateActive,
					createdAt:      now,
					updatedAt:      now,
				})},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusQueued, 1, nil, nil, nil)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, &startedAt, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "RETURNING version",
				columns: []string{"version"},
				rows:    [][]driver.Value{{int64(3)}},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 3, &startedAt, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 3, &startedAt, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 3, &startedAt, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows:    [][]driver.Value{artifactDriverRow(now, expiresAt)},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 3, &startedAt, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
			{
				match:   "FROM diagnostics\nWHERE owner_type=$1",
				columns: diagnosticColumns(),
				rows: [][]driver.Value{{
					"diag-1", "web", "user-1", "", "analysis_run", "run-1", "warning", "worker_partial", "partial success", []byte(`{"step":2}`), []byte(`{"safe":true}`), "corr-1", "retry", completedAt,
				}},
			},
			{
				match:   "RETURNING version",
				columns: []string{"version"},
				rows:    [][]driver.Value{{int64(4)}},
			},
			{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusSucceeded, 4, &startedAt, &completedAt, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
		},
	}

	store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
	if err != nil {
		t.Fatalf("NewSQLStateStore() error = %v", err)
	}
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	collection, err := store.CreateCollection(context.Background(), CollectionRecord{
		ID:        "collection-1",
		Owner:     owner,
		Kind:      CollectionKindUser,
		Name:      "Review",
		Status:    CollectionStatusActive,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}, []string{"media-1"})
	if err != nil {
		t.Fatalf("CreateCollection() error = %v", err)
	}
	if collection.Version != 1 || len(collection.Items) != 1 {
		t.Fatalf("collection = %#v", collection)
	}

	updatedCollection, err := store.UpdateCollection(context.Background(), UpdateCollectionRequest{
		CollectionID:    "collection-1",
		Owner:           owner,
		ExpectedVersion: 1,
		Name:            "Review v2",
		Status:          CollectionStatusArchived,
	}, now)
	if err != nil {
		t.Fatalf("UpdateCollection() error = %v", err)
	}
	if updatedCollection.Name != "Review v2" || updatedCollection.Status != CollectionStatusArchived || updatedCollection.Version != 2 {
		t.Fatalf("updatedCollection = %#v", updatedCollection)
	}

	updatedItems, err := store.UpdateCollectionItems(context.Background(), UpdateCollectionItemsRequest{
		CollectionID:    "collection-1",
		Owner:           owner,
		ExpectedVersion: 2,
		Items:           []CollectionItemRecord{{MediaItemID: "media-1", Position: 1}},
		AddedBy:         "tester",
	}, completedAt)
	if err != nil {
		t.Fatalf("UpdateCollectionItems() error = %v", err)
	}
	if updatedItems.Version != 3 || len(updatedItems.Items) != 1 || updatedItems.Items[0].AddedBy != "tester" {
		t.Fatalf("updatedItems = %#v", updatedItems)
	}

	selection, err := store.CreateSelection(context.Background(), SelectionRecord{
		ID:                 "selection-1",
		Owner:              owner,
		Status:             SelectionStatusSealed,
		SourceCollectionID: "collection-1",
		OptionSnapshotJSON: []byte(`{"duplicate_policy":"keep_all"}`),
		CreatedBy:          "tester",
		CreatedAt:          now,
		SealedAt:           now,
	}, []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}})
	if err != nil {
		t.Fatalf("CreateSelection() error = %v", err)
	}
	if selection.ID != "selection-1" || len(selection.Items) != 1 {
		t.Fatalf("selection = %#v", selection)
	}

	run, err := store.CreateAnalysisRun(context.Background(), AnalysisRunRecord{
		ID:                "run-1",
		Owner:             owner,
		SelectionID:       "selection-1",
		RunType:           "transcription",
		Status:            AnalysisRunStatusQueued,
		Version:           1,
		ParamsJSON:        []byte(`{"language":"ru"}`),
		DeliveryJSON:      []byte(`{"strategy":"polling"}`),
		EvidenceGateState: "not_required",
		CreatedAt:         now,
		ExpiresAt:         &expiresAt,
	}, AnalysisRunTaskRecord{
		ID:            "task-1",
		AnalysisRunID: "run-1",
		WorkerKind:    "transcription",
		TaskType:      "selection.transcription",
		Status:        AnalysisRunTaskStatusQueued,
		AttemptNo:     1,
		CreatedAt:     now,
	}, RunEventRecord{
		ID:            "event-1",
		AnalysisRunID: "run-1",
		EventType:     "analysis_run.created",
		Version:       1,
		PayloadJSON:   []byte(`{"stage":"created"}`),
		Status:        AnalysisRunStatusQueued,
		CreatedAt:     now,
	})
	if err != nil {
		t.Fatalf("CreateAnalysisRun() error = %v", err)
	}
	if run.ID != "run-1" || run.Selection.ID != "selection-1" {
		t.Fatalf("run = %#v", run)
	}

	claimedRun, claimed, err := store.ClaimAnalysisRunTask(context.Background(), "run-1", "transcription", "selection.transcription", "worker-1", startedAt)
	if err != nil {
		t.Fatalf("ClaimAnalysisRunTask() error = %v", err)
	}
	if !claimed || claimedRun.Status != AnalysisRunStatusRunning || claimedRun.Version != 2 {
		t.Fatalf("claimedRun = %#v claimed=%v", claimedRun, claimed)
	}

	progressRun, err := store.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
		ID:            "event-2",
		AnalysisRunID: "run-1",
		EventType:     "analysis_run.progress",
		PayloadJSON:   []byte(`{"step":2}`),
		Status:        AnalysisRunStatusRunning,
		CreatedAt:     completedAt,
	}, completedAt)
	if err != nil {
		t.Fatalf("RecordAnalysisRunProgress() error = %v", err)
	}
	if progressRun.Version != 3 || progressRun.Status != AnalysisRunStatusRunning {
		t.Fatalf("progressRun = %#v", progressRun)
	}

	artifacts, err := store.RecordArtifacts(context.Background(), owner, "run-1", []ArtifactRecord{{
		ID:            "artifact-1",
		Owner:         owner,
		AnalysisRunID: "run-1",
		Kind:          "transcript",
		Status:        ArtifactStatusAvailable,
		ObjectKey:     "artifacts/run-1/transcript.md",
		ContentType:   "text/markdown",
		Checksum:      "sha256:222",
		SizeBytes:     128,
		Visibility:    "owner",
		PreviewJSON:   []byte(`{"available":true}`),
		Retention:     RetentionMetadata{State: RetentionStateActive},
	}}, now)
	if err != nil {
		t.Fatalf("RecordArtifacts() error = %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].ID != "artifact-1" {
		t.Fatalf("artifacts = %#v", artifacts)
	}

	diagnostics, err := store.RecordDiagnostics(context.Background(), owner, "run-1", []DiagnosticRecord{{
		ID:              "diag-1",
		Owner:           owner,
		SubjectType:     "analysis_run",
		SubjectID:       "run-1",
		Severity:        "warning",
		Code:            "worker_partial",
		Message:         "partial success",
		ContextJSON:     []byte(`{"step":2}`),
		SafeAdapterJSON: []byte(`{"safe":true}`),
		CorrelationID:   "corr-1",
		RemediationHint: "retry",
		CreatedAt:       completedAt,
	}}, now)
	if err != nil {
		t.Fatalf("RecordDiagnostics() error = %v", err)
	}
	if len(diagnostics) != 1 || diagnostics[0].Code != "worker_partial" {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}

	finalRun, err := store.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
		ID:            "event-3",
		AnalysisRunID: "run-1",
		EventType:     "analysis_run.finalized",
		PayloadJSON:   []byte(`{"result":"ok"}`),
		Status:        AnalysisRunStatusSucceeded,
		CreatedAt:     completedAt,
	}, completedAt)
	if err != nil {
		t.Fatalf("FinalizeAnalysisRunTask() error = %v", err)
	}
	if finalRun.Status != AnalysisRunStatusSucceeded || finalRun.Version != 4 {
		t.Fatalf("finalRun = %#v", finalRun)
	}

	config.assertExhausted(t)
}

func TestSQLStateStoreExecutionErrorMappings(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 18, 0, 0, 0, time.UTC)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}

	t.Run("create selection maps missing media item", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO selections", affected: 1},
			},
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM media_items mi",
					columns: mediaItemColumns(),
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateSelection(context.Background(), SelectionRecord{
			ID:        "selection-1",
			Owner:     owner,
			Status:    SelectionStatusSealed,
			CreatedBy: "tester",
			CreatedAt: now,
			SealedAt:  now,
		}, []CollectionItemRecord{{MediaItemID: "missing-media", Position: 0}})
		if !errors.Is(err, ErrMediaItemNotFound) {
			t.Fatalf("CreateSelection() error = %v, want ErrMediaItemNotFound", err)
		}
	})

	t.Run("create analysis run maps missing selection", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateAnalysisRun(context.Background(), AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "summary",
			Status:      AnalysisRunStatusQueued,
			Version:     1,
			CreatedAt:   now,
		}, AnalysisRunTaskRecord{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "analysis_runner",
			TaskType:      "selection.analysis",
			Status:        AnalysisRunTaskStatusQueued,
			AttemptNo:     1,
			CreatedAt:     now,
		}, RunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Version:       1,
			CreatedAt:     now,
		})
		if !errors.Is(err, ErrSelectionNotFound) {
			t.Fatalf("CreateAnalysisRun() error = %v, want ErrSelectionNotFound", err)
		}
	})

	t.Run("record progress maps missing active run", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "RETURNING version",
					columns: []string{"version"},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.progress",
			Status:        AnalysisRunStatusRunning,
			CreatedAt:     now,
		}, now)
		if !errors.Is(err, ErrAnalysisRunNotFound) {
			t.Fatalf("RecordAnalysisRunProgress() error = %v, want ErrAnalysisRunNotFound", err)
		}
	})

	t.Run("record artifacts and diagnostics map missing run", func(t *testing.T) {
		t.Parallel()

		missingRunConfig := func() *scriptedRuntimeStoreConfig {
			return &scriptedRuntimeStoreConfig{
				queryResponses: []scriptedQueryResponse{
					{
						match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
						columns: analysisRunColumns(),
					},
				},
			}
		}

		storeArtifacts, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, missingRunConfig()))
		if err != nil {
			t.Fatalf("NewSQLStateStore(artifacts) error = %v", err)
		}
		_, err = storeArtifacts.RecordArtifacts(context.Background(), owner, "run-1", []ArtifactRecord{{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "summary",
			Status:        ArtifactStatusAvailable,
			ContentType:   "text/plain",
			Visibility:    "owner",
		}}, now)
		if !errors.Is(err, ErrAnalysisRunNotFound) {
			t.Fatalf("RecordArtifacts() error = %v, want ErrAnalysisRunNotFound", err)
		}

		storeDiagnostics, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, missingRunConfig()))
		if err != nil {
			t.Fatalf("NewSQLStateStore(diagnostics) error = %v", err)
		}
		_, err = storeDiagnostics.RecordDiagnostics(context.Background(), owner, "run-1", []DiagnosticRecord{{
			ID:          "diag-1",
			Owner:       owner,
			SubjectType: "analysis_run",
			SubjectID:   "run-1",
			Severity:    "warning",
			Code:        "worker_partial",
			Message:     "partial",
			CreatedAt:   now,
		}}, now)
		if !errors.Is(err, ErrAnalysisRunNotFound) {
			t.Fatalf("RecordDiagnostics() error = %v, want ErrAnalysisRunNotFound", err)
		}
	})
}

func TestRuntimeStoreHelperErrorPaths(t *testing.T) {
	t.Parallel()

	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	helperErr := errors.New("db helper failed")

	t.Run("select inbox collection propagates generic errors", func(t *testing.T) {
		t.Parallel()

		db := openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM collections\nWHERE owner_type=$1 AND owner_id=$2",
				columns: collectionColumns(),
				err:     helperErr,
			}},
		})
		err := withTx(context.Background(), db, func(tx *sql.Tx) error {
			_, err := selectInboxCollection(context.Background(), tx, owner)
			return err
		})
		if !errors.Is(err, helperErr) {
			t.Fatalf("selectInboxCollection() error = %v, want helperErr", err)
		}
	})

	t.Run("select analysis run by id propagates generic errors", func(t *testing.T) {
		t.Parallel()

		db := openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				err:     helperErr,
			}},
		})
		err := withTx(context.Background(), db, func(tx *sql.Tx) error {
			_, err := selectAnalysisRunByID(context.Background(), tx, "run-1")
			return err
		})
		if !errors.Is(err, helperErr) {
			t.Fatalf("selectAnalysisRunByID() error = %v, want helperErr", err)
		}
	})

	t.Run("list pending enqueue tasks normalizes non-positive limit", func(t *testing.T) {
		t.Parallel()

		now := time.Date(2026, 5, 11, 18, 30, 0, 0, time.UTC)
		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunTaskColumns(),
				rows: [][]driver.Value{{
					"task-1", "run-1", "analysis_runner", "selection.analysis", AnalysisRunTaskStatusPendingEnqueue, int64(1), "", nil, nil, nil, now,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		tasks, err := store.ListPendingEnqueueTasks(context.Background(), 0)
		if err != nil {
			t.Fatalf("ListPendingEnqueueTasks() error = %v", err)
		}
		if len(tasks) != 1 || tasks[0].TaskType != "selection.analysis" {
			t.Fatalf("tasks = %#v", tasks)
		}
	})
}

func TestRuntimeStoreQueryAndQueueEdgeBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 18, 45, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("query branch failed")

	t.Run("select analysis run by id maps no rows", func(t *testing.T) {
		t.Parallel()

		db := openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
			}},
		})
		err := withTx(context.Background(), db, func(tx *sql.Tx) error {
			_, err := selectAnalysisRunByID(context.Background(), tx, "missing-run")
			return err
		})
		if !errors.Is(err, ErrAnalysisRunNotFound) {
			t.Fatalf("selectAnalysisRunByID(missing) error = %v, want ErrAnalysisRunNotFound", err)
		}
	})

	t.Run("list collection items propagates scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "SELECT media_item_id, position, COALESCE(added_by,''), added_at, removed_at FROM collection_items",
				columns: collectionItemColumns(),
				rows: [][]driver.Value{{
					"media-1", "bad-position", "tester", now, nil,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.listCollectionItems(context.Background(), "collection-1")
		if err == nil {
			t.Fatalf("listCollectionItems() error = nil, want scan failure")
		}
	})

	t.Run("list run events propagates event scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, nil, nil, nil)},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
				{
					match:   "SELECT id, analysis_run_id, event_type, version, payload, COALESCE(status,''), created_at FROM analysis_run_events",
					columns: runEventColumns(),
					rows: [][]driver.Value{{
						"event-1", "run-1", "analysis_run.progress", "bad-version", []byte(`{"step":2}`), AnalysisRunStatusRunning, now,
					}},
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListRunEvents(context.Background(), owner, "run-1")
		if err == nil {
			t.Fatalf("ListRunEvents() error = nil, want scan failure")
		}
	})

	t.Run("list artifacts without run filter skips run lookup", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows:    [][]driver.Value{artifactDriverRow(now, expiresAt)},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		artifacts, err := store.ListArtifacts(context.Background(), owner, "")
		if err != nil {
			t.Fatalf("ListArtifacts() error = %v", err)
		}
		if len(artifacts) != 1 || artifacts[0].ID != "artifact-1" {
			t.Fatalf("artifacts = %#v, want one artifact without owner lookup", artifacts)
		}
	})

	t.Run("list diagnostics propagates query errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM diagnostics\nWHERE owner_type=$1 AND owner_id=$2",
				columns: diagnosticColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListDiagnostics(context.Background(), owner, DiagnosticQuery{Code: "worker_partial"})
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListDiagnostics() error = %v, want stepErr", err)
		}
	})

	t.Run("list analysis run queue normalizes limit and propagates scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunQueueColumns(),
				rows: [][]driver.Value{{
					"run-1", "summary", "analysis_runner", "selection.analysis", AnalysisRunTaskStatusQueued, "bad-version", int64(1), now,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListAnalysisRunQueue(context.Background(), "", "", "", 0)
		if err == nil {
			t.Fatalf("ListAnalysisRunQueue() error = nil, want scan failure")
		}
	})

	t.Run("list analysis run queue propagates query errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunQueueColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListAnalysisRunQueue(context.Background(), AnalysisRunTaskStatusQueued, "summary", "selection.analysis", 5)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListAnalysisRunQueue(query) error = %v, want stepErr", err)
		}
	})

	t.Run("mark analysis run task queued propagates exec errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "UPDATE analysis_run_tasks\nSET status='queued'",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		err = store.MarkAnalysisRunTaskQueued(context.Background(), "run-1", "selection.analysis", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("MarkAnalysisRunTaskQueued() error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreTaskQueueErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 19, 15, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	stepErr := errors.New("task queue branch failed")

	t.Run("list pending enqueue tasks propagates query errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunTaskColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListPendingEnqueueTasks(context.Background(), 10)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListPendingEnqueueTasks() error = %v, want stepErr", err)
		}
	})

	t.Run("list pending enqueue tasks propagates scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_run_tasks t\nJOIN analysis_runs ar",
				columns: analysisRunTaskColumns(),
				rows: [][]driver.Value{{
					"task-1", "run-1", "analysis_runner", "selection.analysis", AnalysisRunTaskStatusPendingEnqueue, "bad-attempt", "", nil, nil, nil, now,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListPendingEnqueueTasks(context.Background(), 10)
		if err == nil {
			t.Fatalf("ListPendingEnqueueTasks() error = nil, want scan failure")
		}
	})

	t.Run("claim analysis run task propagates run-status update errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusQueued, 1, nil, nil, nil)},
			}},
			execResponses: []scriptedExecResponse{{
				match: "UPDATE analysis_runs\nSET status='running'",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, _, err = store.ClaimAnalysisRunTask(context.Background(), "run-1", "analysis_runner", "selection.analysis", "worker-1", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ClaimAnalysisRunTask() error = %v, want stepErr", err)
		}
	})

	t.Run("claim analysis run task propagates refreshed-run lookup errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusQueued, 1, nil, nil, nil)},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{{
				match:    "UPDATE analysis_runs\nSET status='running'",
				affected: 1,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, _, err = store.ClaimAnalysisRunTask(context.Background(), "run-1", "analysis_runner", "selection.analysis", "worker-1", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ClaimAnalysisRunTask() error = %v, want stepErr", err)
		}
	})

	t.Run("claim analysis run task ignores selection refresh errors after a successful claim", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusQueued, 1, nil, nil, nil)},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, &now, nil, nil)},
				},
				{
					match:   "FROM selections s",
					columns: selectionColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{{
				match:    "UPDATE analysis_runs\nSET status='running'",
				affected: 1,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		run, claimed, err := store.ClaimAnalysisRunTask(context.Background(), "run-1", "analysis_runner", "selection.analysis", "worker-1", now)
		if err != nil {
			t.Fatalf("ClaimAnalysisRunTask(selection refresh ignored) error = %v", err)
		}
		if !claimed {
			t.Fatalf("claimed = false, want true")
		}
		if run.ID != "run-1" || run.Status != AnalysisRunStatusRunning {
			t.Fatalf("run = %#v, want claimed running run", run)
		}
		if run.Selection.ID != "" {
			t.Fatalf("run.Selection = %#v, want zero selection on refresh error", run.Selection)
		}
	})

	t.Run("claim analysis run task propagates claimed-row scan failures", func(t *testing.T) {
		t.Parallel()

		invalidRunRow := []driver.Value{
			"run-1", "web", "user-1", "", "selection-1", "transcription", AnalysisRunStatusQueued, "bad-version",
			[]byte(`{"language":"ru"}`), []byte(`{"strategy":"polling"}`), "not_required", now, nil, nil, nil, expiresAt,
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{invalidRunRow},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, _, err := store.ClaimAnalysisRunTask(context.Background(), "run-1", "analysis_runner", "selection.analysis", "worker-1", now); err == nil {
			t.Fatalf("ClaimAnalysisRunTask(scan) error = nil, want scan failure")
		}
	})

	t.Run("claim analysis run task propagates refreshed-run lookup errors after no claimed row", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_run_tasks t\nSET status='claimed'",
					columns: analysisRunColumns(),
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		if _, _, err := store.ClaimAnalysisRunTask(context.Background(), "run-1", "analysis_runner", "selection.analysis", "worker-1", now); !errors.Is(err, stepErr) {
			t.Fatalf("ClaimAnalysisRunTask(no claimed row lookup) error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreRunLookupAndFinalizeErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 19, 30, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("run-lookup-finalize branch failed")

	t.Run("analysis run lookups propagate non-not-found scan errors", func(t *testing.T) {
		t.Parallel()

		invalidRunRow := []driver.Value{
			"run-1", "web", "user-1", "", "selection-1", "transcription", AnalysisRunStatusRunning, "bad-version",
			[]byte(`{"language":"ru"}`), []byte(`{"strategy":"polling"}`), "not_required", now, nil, nil, nil, expiresAt,
		}

		storeByID, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1::uuid",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{invalidRunRow},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(by id) error = %v", err)
		}
		if _, err := storeByID.GetAnalysisRunByID(context.Background(), "run-1"); err == nil {
			t.Fatalf("GetAnalysisRunByID(scan) error = nil, want scan failure")
		}

		storeByOwner, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{invalidRunRow},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(by owner) error = %v", err)
		}
		if _, err := storeByOwner.GetAnalysisRun(context.Background(), owner, "run-1"); err == nil {
			t.Fatalf("GetAnalysisRun(scan) error = nil, want scan failure")
		}
	})

	t.Run("run-gated list helpers propagate lookup failures", func(t *testing.T) {
		t.Parallel()

		storeEvents, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(events) error = %v", err)
		}
		if _, err := storeEvents.ListRunEvents(context.Background(), owner, "run-1"); !errors.Is(err, stepErr) {
			t.Fatalf("ListRunEvents(lookup) error = %v, want stepErr", err)
		}

		storeArtifacts, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(artifacts) error = %v", err)
		}
		if _, err := storeArtifacts.ListArtifacts(context.Background(), owner, "run-1"); !errors.Is(err, stepErr) {
			t.Fatalf("ListArtifacts(lookup) error = %v, want stepErr", err)
		}
	})

	t.Run("list diagnostics propagates scan failures", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM diagnostics\nWHERE owner_type=$1 AND owner_id=$2",
				columns: diagnosticColumns(),
				rows: [][]driver.Value{{
					"diag-1", "web", "user-1", "", "analysis_run", "run-1", "warning", "worker_partial", "partial success", []byte(`{"step":1}`), []byte(`{"safe":true}`), "corr-1", "retry", "bad-time",
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(diagnostics) error = %v", err)
		}
		if _, err := store.ListDiagnostics(context.Background(), owner, DiagnosticQuery{}); err == nil {
			t.Fatalf("ListDiagnostics(scan) error = nil, want scan failure")
		}
	})

	t.Run("record progress propagates version scan failures", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "RETURNING version",
				columns: []string{"version"},
				rows:    [][]driver.Value{{"bad-version"}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(progress) error = %v", err)
		}
		if _, err := store.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
			ID:            "event-progress",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.progress",
			Status:        AnalysisRunStatusRunning,
			CreatedAt:     now,
		}, now); err == nil {
			t.Fatalf("RecordAnalysisRunProgress(scan) error = nil, want scan failure")
		}
	})

	t.Run("finalize propagates lookup, scan, task-update, and event-insert failures", func(t *testing.T) {
		t.Parallel()

		storeLookupErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "UPDATE analysis_runs\nSET status=$5",
					columns: []string{"version"},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(finalize lookup) error = %v", err)
		}
		if _, err := storeLookupErr.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-lookup",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     now,
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("FinalizeAnalysisRunTask(lookup) error = %v, want stepErr", err)
		}

		storeScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "UPDATE analysis_runs\nSET status=$5",
				columns: []string{"version"},
				rows:    [][]driver.Value{{"bad-version"}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(finalize scan) error = %v", err)
		}
		if _, err := storeScanErr.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-scan",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     now,
		}, now); err == nil {
			t.Fatalf("FinalizeAnalysisRunTask(scan) error = nil, want scan failure")
		}

		storeTaskErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "UPDATE analysis_runs\nSET status=$5",
				columns: []string{"version"},
				rows:    [][]driver.Value{{int64(3)}},
			}},
			execResponses: []scriptedExecResponse{{
				match: "UPDATE analysis_run_tasks\nSET status=$2, finalized_at=$3, heartbeat_at=$3",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(finalize task) error = %v", err)
		}
		if _, err := storeTaskErr.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-task",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     now,
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("FinalizeAnalysisRunTask(task update) error = %v, want stepErr", err)
		}

		storeEventErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "UPDATE analysis_runs\nSET status=$5",
				columns: []string{"version"},
				rows:    [][]driver.Value{{int64(3)}},
			}},
			execResponses: []scriptedExecResponse{
				{match: "UPDATE analysis_run_tasks\nSET status=$2, finalized_at=$3, heartbeat_at=$3", affected: 1},
				{match: "INSERT INTO analysis_run_events", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(finalize event) error = %v", err)
		}
		if _, err := storeEventErr.FinalizeAnalysisRunTask(context.Background(), owner, "run-1", AnalysisRunStatusSucceeded, RunEventRecord{
			ID:            "event-insert",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.finalized",
			Status:        AnalysisRunStatusSucceeded,
			CreatedAt:     now,
		}, now); !errors.Is(err, stepErr) {
			t.Fatalf("FinalizeAnalysisRunTask(event insert) error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreRetentionAndOrphanErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 19, 45, 0, 0, time.UTC)
	stepErr := errors.New("retention-orphan branch failed")

	t.Run("apply retention policies propagates later step errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items mi", affected: 1},
				{match: "UPDATE collection_items ci", affected: 1},
				{match: "UPDATE collections c", affected: 1},
				{match: "UPDATE selections s", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ApplyRetentionPolicies(context.Background(), now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ApplyRetentionPolicies() error = %v, want stepErr", err)
		}
	})

	t.Run("apply retention policies propagates each remaining step error", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			name  string
			execs []scriptedExecResponse
		}{
			{
				name: "media-items step",
				execs: []scriptedExecResponse{
					{match: "UPDATE media_items mi", err: stepErr},
				},
			},
			{
				name: "collection-items step",
				execs: []scriptedExecResponse{
					{match: "UPDATE media_items mi", affected: 1},
					{match: "UPDATE collection_items ci", err: stepErr},
				},
			},
			{
				name: "collections step",
				execs: []scriptedExecResponse{
					{match: "UPDATE media_items mi", affected: 1},
					{match: "UPDATE collection_items ci", affected: 1},
					{match: "UPDATE collections c", err: stepErr},
				},
			},
			{
				name: "analysis-runs step",
				execs: []scriptedExecResponse{
					{match: "UPDATE media_items mi", affected: 1},
					{match: "UPDATE collection_items ci", affected: 1},
					{match: "UPDATE collections c", affected: 1},
					{match: "UPDATE selections s", affected: 1},
					{match: "UPDATE analysis_runs\nSET status='expired'", err: stepErr},
				},
			},
			{
				name: "artifacts step",
				execs: []scriptedExecResponse{
					{match: "UPDATE media_items mi", affected: 1},
					{match: "UPDATE collection_items ci", affected: 1},
					{match: "UPDATE collections c", affected: 1},
					{match: "UPDATE selections s", affected: 1},
					{match: "UPDATE analysis_runs\nSET status='expired'", affected: 1},
					{match: "UPDATE artifacts\nSET status='expired'", err: stepErr},
				},
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
					execResponses: tc.execs,
				}))
				if err != nil {
					t.Fatalf("NewSQLStateStore() error = %v", err)
				}

				if _, err := store.ApplyRetentionPolicies(context.Background(), now); !errors.Is(err, stepErr) {
					t.Fatalf("ApplyRetentionPolicies() error = %v, want stepErr", err)
				}
			})
		}
	})

	t.Run("detect orphan objects propagates query errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "SELECT 'source', s.id::text",
				columns: orphanColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.DetectOrphanObjects(context.Background())
		if !errors.Is(err, stepErr) {
			t.Fatalf("DetectOrphanObjects() error = %v, want stepErr", err)
		}
	})

	t.Run("detect orphan objects propagates scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "SELECT 'source', s.id::text",
				columns: orphanColumns(),
				rows: [][]driver.Value{{
					"source", "source-1", "web", "user-1", "", nil, "sources/source-1/source.txt", "deleted_media_source",
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.DetectOrphanObjects(context.Background())
		if err == nil {
			t.Fatalf("DetectOrphanObjects() error = nil, want scan failure")
		}
	})

	t.Run("record orphan cleanup covers deleted source update path", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items\nSET retention_state=$1", affected: 1},
				{match: "UPDATE sources SET expires_at=$1", affected: 1},
				{match: "INSERT INTO diagnostics", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		err = store.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{
			SubjectType: "source",
			SubjectID:   "source-1",
			Owner:       OwnerScope{OwnerType: "web", OwnerID: "user-1"},
			Bucket:      "sources",
			ObjectKey:   "sources/source-1/source.txt",
			Reason:      "expired_media_source",
		}, true, "deleted from storage", now)
		if err != nil {
			t.Fatalf("RecordOrphanObjectCleanup(source deleted) error = %v", err)
		}
	})

	t.Run("record orphan cleanup propagates diagnostics insert errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE artifacts\nSET status=CASE WHEN $1 THEN 'deleted' ELSE status END", affected: 1},
				{match: "INSERT INTO diagnostics", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		err = store.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{
			SubjectType: "artifact",
			SubjectID:   "artifact-1",
			Owner:       OwnerScope{OwnerType: "web", OwnerID: "user-1"},
			Bucket:      "artifacts",
			ObjectKey:   "artifacts/run-1/report.md",
			Reason:      "deleted_artifact",
		}, false, "metadata only", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordOrphanObjectCleanup(artifact) error = %v, want stepErr", err)
		}
	})

	t.Run("record orphan cleanup propagates deleted source expiry update errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "UPDATE media_items\nSET retention_state=$1", affected: 1},
				{match: "UPDATE sources SET expires_at=$1", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		err = store.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{
			SubjectType: "source",
			SubjectID:   "source-1",
			Owner:       OwnerScope{OwnerType: "web", OwnerID: "user-1"},
			Bucket:      "sources",
			ObjectKey:   "sources/source-1/source.txt",
			Reason:      "expired_media_source",
		}, true, "deleted from storage", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordOrphanObjectCleanup(source expires_at) error = %v, want stepErr", err)
		}
	})

	t.Run("record orphan cleanup propagates primary source and artifact update errors", func(t *testing.T) {
		t.Parallel()

		storeSourceErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "UPDATE media_items\nSET retention_state=$1",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(source update) error = %v", err)
		}

		err = storeSourceErr.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{
			SubjectType: "source",
			SubjectID:   "source-1",
			Owner:       OwnerScope{OwnerType: "web", OwnerID: "user-1"},
			Bucket:      "sources",
			ObjectKey:   "sources/source-1/source.txt",
			Reason:      "expired_media_source",
		}, false, "metadata only", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordOrphanObjectCleanup(source update) error = %v, want stepErr", err)
		}

		storeArtifactErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{{
				match: "UPDATE artifacts\nSET status=CASE WHEN $1 THEN 'deleted' ELSE status END",
				err:   stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(artifact update) error = %v", err)
		}

		err = storeArtifactErr.RecordOrphanObjectCleanup(context.Background(), OrphanObjectRecord{
			SubjectType: "artifact",
			SubjectID:   "artifact-1",
			Owner:       OwnerScope{OwnerType: "web", OwnerID: "user-1"},
			Bucket:      "artifacts",
			ObjectKey:   "artifacts/run-1/report.md",
			Reason:      "deleted_artifact",
		}, true, "delete failed at provider", now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordOrphanObjectCleanup(artifact update) error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreArtifactAndOpsDiagnosticErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 20, 0, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("artifact-diagnostics branch failed")

	t.Run("list artifacts propagates query errors after run lookup", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, nil, nil, nil)},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
				{
					match:   "FROM artifacts a",
					columns: artifactColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListArtifacts(context.Background(), owner, "run-1")
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListArtifacts() error = %v, want stepErr", err)
		}
	})

	t.Run("list artifacts propagates scan errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				rows: [][]driver.Value{{
					"artifact-1", "web", "user-1", "", "run-1", "transcript", ArtifactStatusAvailable, "artifacts/run-1/transcript.md", "text/markdown", "sha256:222", "bad-size", "owner", []byte(`{"available":true}`), RetentionStateActive, "", now, expiresAt, nil,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListArtifacts(context.Background(), owner, "")
		if err == nil {
			t.Fatalf("ListArtifacts() error = nil, want scan failure")
		}
	})

	t.Run("get artifact and by-id propagate generic scan errors", func(t *testing.T) {
		t.Parallel()

		storeByOwner, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(owner) error = %v", err)
		}
		_, err = storeByOwner.GetArtifact(context.Background(), owner, "artifact-1")
		if !errors.Is(err, stepErr) {
			t.Fatalf("GetArtifact() error = %v, want stepErr", err)
		}

		storeByID, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM artifacts a",
				columns: artifactColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(by-id) error = %v", err)
		}
		_, err = storeByID.GetArtifactByID(context.Background(), "artifact-1")
		if !errors.Is(err, stepErr) {
			t.Fatalf("GetArtifactByID() error = %v, want stepErr", err)
		}
	})

	t.Run("list operational diagnostics propagates query and scan errors", func(t *testing.T) {
		t.Parallel()

		storeQueryErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM diagnostics\nWHERE code IN",
				columns: diagnosticColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(query) error = %v", err)
		}
		_, err = storeQueryErr.ListOperationalDiagnostics(context.Background(), []string{"orphan_object_cleanup_failed"})
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListOperationalDiagnostics(query) error = %v, want stepErr", err)
		}

		storeScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM diagnostics\nWHERE code IN",
				columns: diagnosticColumns(),
				rows: [][]driver.Value{{
					"diag-1", "system", "cleanup", "", "artifact", "artifact-1", "error", "orphan_object_cleanup_failed", "delete failed", []byte(`{"bucket":"artifacts"}`), []byte(`{}`), "", "", "bad-time",
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(scan) error = %v", err)
		}
		_, err = storeScanErr.ListOperationalDiagnostics(context.Background(), []string{"orphan_object_cleanup_failed"})
		if err == nil {
			t.Fatalf("ListOperationalDiagnostics(scan) error = nil, want scan failure")
		}
	})
}

func TestRuntimeStoreSelectionAndRunReadErrorBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 20, 15, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("selection-run-read branch failed")

	t.Run("get selection propagates item query and scan errors", func(t *testing.T) {
		t.Parallel()

		storeQueryErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(query) error = %v", err)
		}

		_, err = storeQueryErr.GetSelection(context.Background(), owner, "selection-1")
		if !errors.Is(err, stepErr) {
			t.Fatalf("GetSelection(query) error = %v, want stepErr", err)
		}

		storeScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows: [][]driver.Value{{
						"selection-item-1", "bad-position", "media-1", "text", []byte(`{}`), "source.txt", MediaStatusReady, []byte(`{}`), []byte(`{}`), []byte(`[]`),
					}},
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(scan) error = %v", err)
		}

		_, err = storeScanErr.GetSelection(context.Background(), owner, "selection-1")
		if err == nil {
			t.Fatalf("GetSelection(scan) error = nil, want scan failure")
		}
	})

	t.Run("list analysis runs propagates query and scan errors", func(t *testing.T) {
		t.Parallel()

		storeQueryErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE owner_type=$1 AND owner_id=$2",
				columns: analysisRunColumns(),
				err:     stepErr,
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(query) error = %v", err)
		}

		_, err = storeQueryErr.ListAnalysisRuns(context.Background(), owner)
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListAnalysisRuns(query) error = %v, want stepErr", err)
		}

		storeScanErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "FROM analysis_runs\nWHERE owner_type=$1 AND owner_id=$2",
				columns: analysisRunColumns(),
				rows: [][]driver.Value{{
					"run-1", "web", "user-1", "", "selection-1", "transcription", AnalysisRunStatusRunning, "bad-version", []byte(`{"language":"ru"}`), []byte(`{"strategy":"polling"}`), "not_required", now, nil, nil, nil, expiresAt,
				}},
			}},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(scan) error = %v", err)
		}

		_, err = storeScanErr.ListAnalysisRuns(context.Background(), owner)
		if err == nil {
			t.Fatalf("ListAnalysisRuns(scan) error = nil, want scan failure")
		}
	})

	t.Run("list run events propagates event query errors", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
					columns: analysisRunColumns(),
					rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, nil, nil, nil)},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
				{
					match:   "SELECT id, analysis_run_id, event_type, version, payload, COALESCE(status,''), created_at FROM analysis_run_events",
					columns: runEventColumns(),
					err:     stepErr,
				},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.ListRunEvents(context.Background(), owner, "run-1")
		if !errors.Is(err, stepErr) {
			t.Fatalf("ListRunEvents(query) error = %v, want stepErr", err)
		}
	})
}

func TestRuntimeStoreExecutionRefreshAndStepBranches(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 20, 30, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	size := int64(42)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("execution-refresh branch failed")

	t.Run("create selection propagates refresh failure after successful inserts", func(t *testing.T) {
		t.Parallel()

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM media_items mi",
					columns: mediaItemColumns(),
					rows: [][]driver.Value{mediaItemDriverRow(mediaItemDriverRowInput{
						id:             "media-1",
						ownerType:      "web",
						ownerID:        "user-1",
						sourceID:       "source-1",
						originType:     "object",
						objectKey:      "sources/source-1/source.txt",
						checksum:       "sha256:111",
						sizeBytes:      &size,
						mimeType:       "text/plain",
						kind:           "text",
						status:         MediaStatusReady,
						displayName:    "source.txt",
						metadataJSON:   []byte(`{"tag":"one"}`),
						retentionState: RetentionStateActive,
						createdAt:      now,
						updatedAt:      now,
					})},
				},
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO selections", affected: 1},
				{match: "INSERT INTO selection_items", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateSelection(context.Background(), SelectionRecord{
			ID:                 "selection-1",
			Owner:              owner,
			Status:             SelectionStatusSealed,
			SourceCollectionID: "collection-1",
			OptionSnapshotJSON: []byte(`{"duplicate_policy":"keep_all"}`),
			CreatedBy:          "tester",
			CreatedAt:          now,
			SealedAt:           now,
		}, []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateSelection() error = %v, want stepErr", err)
		}
	})

	t.Run("create analysis run propagates run and task insert failures", func(t *testing.T) {
		t.Parallel()

		baseQueries := []scriptedQueryResponse{
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
		}

		storeRunInsert, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: append([]scriptedQueryResponse(nil), baseQueries...),
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_runs", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(run insert) error = %v", err)
		}
		_, err = storeRunInsert.CreateAnalysisRun(context.Background(), AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "transcription",
			Status:      AnalysisRunStatusQueued,
			Version:     1,
			CreatedAt:   now,
			ExpiresAt:   &expiresAt,
		}, AnalysisRunTaskRecord{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "transcription",
			TaskType:      "selection.transcription",
			Status:        AnalysisRunTaskStatusQueued,
			AttemptNo:     1,
			CreatedAt:     now,
		}, RunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Version:       1,
			Status:        AnalysisRunStatusQueued,
			CreatedAt:     now,
		})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateAnalysisRun(run insert) error = %v, want stepErr", err)
		}

		storeTaskInsert, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: append([]scriptedQueryResponse(nil), baseQueries...),
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_runs", affected: 1},
				{match: "INSERT INTO analysis_run_tasks", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(task insert) error = %v", err)
		}
		_, err = storeTaskInsert.CreateAnalysisRun(context.Background(), AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "transcription",
			Status:      AnalysisRunStatusQueued,
			Version:     1,
			CreatedAt:   now,
			ExpiresAt:   &expiresAt,
		}, AnalysisRunTaskRecord{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "transcription",
			TaskType:      "selection.transcription",
			Status:        AnalysisRunTaskStatusQueued,
			AttemptNo:     1,
			CreatedAt:     now,
		}, RunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Version:       1,
			Status:        AnalysisRunStatusQueued,
			CreatedAt:     now,
		})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateAnalysisRun(task insert) error = %v, want stepErr", err)
		}
	})

	t.Run("record analysis run progress propagates heartbeat and refresh failures", func(t *testing.T) {
		t.Parallel()

		storeHeartbeatErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{{
				match:   "RETURNING version",
				columns: []string{"version"},
				rows:    [][]driver.Value{{int64(3)}},
			}},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_run_events", affected: 1},
				{match: "UPDATE analysis_run_tasks\nSET heartbeat_at=$2", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(heartbeat) error = %v", err)
		}
		_, err = storeHeartbeatErr.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
			ID:            "event-2",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.progress",
			Status:        AnalysisRunStatusRunning,
			CreatedAt:     now,
		}, now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordAnalysisRunProgress(heartbeat) error = %v, want stepErr", err)
		}

		storeRefreshErr, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: []scriptedQueryResponse{
				{
					match:   "RETURNING version",
					columns: []string{"version"},
					rows:    [][]driver.Value{{int64(3)}},
				},
				{
					match:   "FROM analysis_runs\nWHERE id=$1::uuid",
					columns: analysisRunColumns(),
					err:     stepErr,
				},
			},
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_run_events", affected: 1},
				{match: "UPDATE analysis_run_tasks\nSET heartbeat_at=$2", affected: 1},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(refresh) error = %v", err)
		}
		_, err = storeRefreshErr.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
			ID:            "event-2",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.progress",
			Status:        AnalysisRunStatusRunning,
			CreatedAt:     now,
		}, now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordAnalysisRunProgress(refresh) error = %v, want stepErr", err)
		}
	})
}

func TestSQLStateStoreExecutionStepErrors(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 11, 19, 0, 0, 0, time.UTC)
	expiresAt := now.Add(2 * time.Hour)
	size := int64(42)
	owner := OwnerScope{OwnerType: "web", OwnerID: "user-1"}
	stepErr := errors.New("step failed")

	t.Run("create selection propagates selection-item insert error", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO selections", affected: 1},
				{match: "INSERT INTO selection_items", err: stepErr},
			},
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM media_items mi",
					columns: mediaItemColumns(),
					rows: [][]driver.Value{mediaItemDriverRow(mediaItemDriverRowInput{
						id:             "media-1",
						ownerType:      "web",
						ownerID:        "user-1",
						sourceID:       "source-1",
						originType:     "object",
						objectKey:      "sources/source-1/source.txt",
						checksum:       "sha256:111",
						sizeBytes:      &size,
						mimeType:       "text/plain",
						kind:           "text",
						status:         MediaStatusReady,
						displayName:    "source.txt",
						metadataJSON:   []byte(`{"tag":"one"}`),
						retentionState: RetentionStateActive,
						createdAt:      now,
						updatedAt:      now,
					})},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateSelection(context.Background(), SelectionRecord{
			ID:        "selection-1",
			Owner:     owner,
			Status:    SelectionStatusSealed,
			CreatedBy: "tester",
			CreatedAt: now,
			SealedAt:  now,
		}, []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateSelection() error = %v, want stepErr", err)
		}
	})

	t.Run("create selection propagates selection insert error", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO selections", err: stepErr},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateSelection(context.Background(), SelectionRecord{
			ID:        "selection-1",
			Owner:     owner,
			Status:    SelectionStatusSealed,
			CreatedBy: "tester",
			CreatedAt: now,
			SealedAt:  now,
		}, []CollectionItemRecord{{MediaItemID: "media-1", Position: 0}})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateSelection(insert) error = %v, want stepErr", err)
		}
	})

	t.Run("create analysis run propagates event insert error", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_runs", affected: 1},
				{match: "INSERT INTO analysis_run_tasks", affected: 1},
				{match: "INSERT INTO analysis_run_events", err: stepErr},
			},
			queryResponses: []scriptedQueryResponse{
				{
					match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
					columns: selectionColumns(),
					rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
				},
				{
					match:   "FROM selection_items WHERE selection_id=$1",
					columns: selectionItemColumns(),
					rows:    [][]driver.Value{selectionItemDriverRow(now)},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.CreateAnalysisRun(context.Background(), AnalysisRunRecord{
			ID:          "run-1",
			Owner:       owner,
			SelectionID: "selection-1",
			RunType:     "summary",
			Status:      AnalysisRunStatusQueued,
			Version:     1,
			CreatedAt:   now,
		}, AnalysisRunTaskRecord{
			ID:            "task-1",
			AnalysisRunID: "run-1",
			WorkerKind:    "analysis_runner",
			TaskType:      "selection.analysis",
			Status:        AnalysisRunTaskStatusQueued,
			AttemptNo:     1,
			CreatedAt:     now,
		}, RunEventRecord{
			ID:            "event-1",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.created",
			Version:       1,
			CreatedAt:     now,
		})
		if !errors.Is(err, stepErr) {
			t.Fatalf("CreateAnalysisRun() error = %v, want stepErr", err)
		}
	})

	t.Run("record progress propagates event insert error", func(t *testing.T) {
		t.Parallel()

		config := &scriptedRuntimeStoreConfig{
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO analysis_run_events", err: stepErr},
			},
			queryResponses: []scriptedQueryResponse{
				{
					match:   "RETURNING version",
					columns: []string{"version"},
					rows:    [][]driver.Value{{int64(2)}},
				},
			},
		}

		store, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, config))
		if err != nil {
			t.Fatalf("NewSQLStateStore() error = %v", err)
		}

		_, err = store.RecordAnalysisRunProgress(context.Background(), owner, "run-1", RunEventRecord{
			ID:            "event-2",
			AnalysisRunID: "run-1",
			EventType:     "analysis_run.progress",
			Status:        AnalysisRunStatusRunning,
			CreatedAt:     now,
		}, now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordAnalysisRunProgress() error = %v, want stepErr", err)
		}
	})

	t.Run("record artifacts and diagnostics propagate insert errors", func(t *testing.T) {
		t.Parallel()

		baseQueryResponses := []scriptedQueryResponse{
			{
				match:   "FROM analysis_runs\nWHERE id=$1 AND owner_type=$2",
				columns: analysisRunColumns(),
				rows:    [][]driver.Value{analysisRunDriverRowWithState(now, expiresAt, AnalysisRunStatusRunning, 2, nil, nil, nil)},
			},
			{
				match:   "FROM selections\nWHERE id=$1 AND owner_type=$2",
				columns: selectionColumns(),
				rows:    [][]driver.Value{selectionDriverRow("selection-1", "collection-1", now)},
			},
			{
				match:   "FROM selection_items WHERE selection_id=$1",
				columns: selectionItemColumns(),
				rows:    [][]driver.Value{selectionItemDriverRow(now)},
			},
		}

		storeArtifacts, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: append([]scriptedQueryResponse(nil), baseQueryResponses...),
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO artifacts", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(artifacts) error = %v", err)
		}
		_, err = storeArtifacts.RecordArtifacts(context.Background(), owner, "run-1", []ArtifactRecord{{
			ID:            "artifact-1",
			Owner:         owner,
			AnalysisRunID: "run-1",
			Kind:          "summary",
			Status:        ArtifactStatusAvailable,
			ContentType:   "text/plain",
			Visibility:    "owner",
		}}, now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordArtifacts() error = %v, want stepErr", err)
		}

		storeDiagnostics, err := NewSQLStateStore(openScriptedRuntimeStoreDB(t, &scriptedRuntimeStoreConfig{
			queryResponses: append([]scriptedQueryResponse(nil), baseQueryResponses...),
			execResponses: []scriptedExecResponse{
				{match: "INSERT INTO diagnostics", err: stepErr},
			},
		}))
		if err != nil {
			t.Fatalf("NewSQLStateStore(diagnostics) error = %v", err)
		}
		_, err = storeDiagnostics.RecordDiagnostics(context.Background(), owner, "run-1", []DiagnosticRecord{{
			ID:          "diag-1",
			Owner:       owner,
			SubjectType: "analysis_run",
			SubjectID:   "run-1",
			Severity:    "warning",
			Code:        "worker_partial",
			Message:     "partial",
			CreatedAt:   now,
		}}, now)
		if !errors.Is(err, stepErr) {
			t.Fatalf("RecordDiagnostics() error = %v, want stepErr", err)
		}
	})
}

type scriptedRuntimeStoreConfig struct {
	mu             sync.Mutex
	queryResponses []scriptedQueryResponse
	execResponses  []scriptedExecResponse
}

type scriptedQueryResponse struct {
	match   string
	columns []string
	rows    [][]driver.Value
	err     error
}

type scriptedExecResponse struct {
	match     string
	affected  int64
	err       error
	checkArgs func([]driver.NamedValue) error
}

type scriptedRuntimeStoreDriver struct {
	config *scriptedRuntimeStoreConfig
}

type scriptedRuntimeStoreConn struct {
	config *scriptedRuntimeStoreConfig
}

type scriptedRuntimeStoreTx struct{}

type scriptedRuntimeStoreRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

type scriptedRowsAffected int64

var scriptedRuntimeStoreDriverSeq atomic.Int64

func openScriptedRuntimeStoreDB(t *testing.T, config *scriptedRuntimeStoreConfig) *sql.DB {
	t.Helper()

	name := fmt.Sprintf("runtime-store-media-%d", scriptedRuntimeStoreDriverSeq.Add(1))
	sql.Register(name, &scriptedRuntimeStoreDriver{config: config})

	db, err := sql.Open(name, "ignored")
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", name, err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func (d *scriptedRuntimeStoreDriver) Open(string) (driver.Conn, error) {
	return &scriptedRuntimeStoreConn{config: d.config}, nil
}

func (c *scriptedRuntimeStoreConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *scriptedRuntimeStoreConn) Close() error {
	return nil
}

func (c *scriptedRuntimeStoreConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *scriptedRuntimeStoreConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &scriptedRuntimeStoreTx{}, nil
}

func (c *scriptedRuntimeStoreConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	response, err := c.config.nextExec(query)
	if err != nil {
		return nil, err
	}
	if response.checkArgs != nil {
		if err := response.checkArgs(args); err != nil {
			return nil, err
		}
	}
	if response.err != nil {
		return nil, response.err
	}
	return scriptedRowsAffected(response.affected), nil
}

func (c *scriptedRuntimeStoreConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	response, err := c.config.nextQuery(query)
	if err != nil {
		return nil, err
	}
	if response.err != nil {
		return nil, response.err
	}
	return &scriptedRuntimeStoreRows{columns: response.columns, rows: response.rows}, nil
}

func (tx *scriptedRuntimeStoreTx) Commit() error {
	return nil
}

func (tx *scriptedRuntimeStoreTx) Rollback() error {
	return nil
}

func (r *scriptedRuntimeStoreRows) Columns() []string {
	return r.columns
}

func (r *scriptedRuntimeStoreRows) Close() error {
	return nil
}

func (r *scriptedRuntimeStoreRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	for idx := range row {
		dest[idx] = row[idx]
	}
	r.index++
	return nil
}

func (r scriptedRowsAffected) LastInsertId() (int64, error) {
	return 0, nil
}

func (r scriptedRowsAffected) RowsAffected() (int64, error) {
	return int64(r), nil
}

func (c *scriptedRuntimeStoreConfig) nextQuery(query string) (scriptedQueryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.queryResponses) == 0 {
		return scriptedQueryResponse{}, fmt.Errorf("unexpected query: %s", compactSQL(query))
	}
	response := c.queryResponses[0]
	c.queryResponses = c.queryResponses[1:]
	if !strings.Contains(query, response.match) {
		return scriptedQueryResponse{}, fmt.Errorf("unexpected query: got %q want substring %q", compactSQL(query), response.match)
	}
	return response, nil
}

func (c *scriptedRuntimeStoreConfig) nextExec(query string) (scriptedExecResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.execResponses) == 0 {
		return scriptedExecResponse{}, fmt.Errorf("unexpected exec: %s", compactSQL(query))
	}
	response := c.execResponses[0]
	c.execResponses = c.execResponses[1:]
	if !strings.Contains(query, response.match) {
		return scriptedExecResponse{}, fmt.Errorf("unexpected exec: got %q want substring %q", compactSQL(query), response.match)
	}
	return response, nil
}

func (c *scriptedRuntimeStoreConfig) assertExhausted(t *testing.T) {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.queryResponses) != 0 || len(c.execResponses) != 0 {
		t.Fatalf("scripted responses not exhausted: queries=%d execs=%d", len(c.queryResponses), len(c.execResponses))
	}
}

type mediaItemDriverRowInput struct {
	id             string
	ownerType      string
	ownerID        string
	sourceID       string
	originType     string
	objectKey      string
	checksum       string
	sizeBytes      *int64
	mimeType       string
	kind           string
	status         string
	displayName    string
	metadataJSON   []byte
	retentionState string
	createdAt      time.Time
	updatedAt      time.Time
	expiresAt      *time.Time
	deletedAt      *time.Time
}

func mediaItemDriverRow(input mediaItemDriverRowInput) []driver.Value {
	var expiresAt any
	if input.expiresAt != nil {
		expiresAt = *input.expiresAt
	}
	var deletedAt any
	if input.deletedAt != nil {
		deletedAt = *input.deletedAt
	}
	var size any
	if input.sizeBytes != nil {
		size = *input.sizeBytes
	}
	return []driver.Value{
		input.id,
		input.ownerType,
		input.ownerID,
		"",
		input.sourceID,
		input.originType,
		"",
		input.objectKey,
		"",
		input.checksum,
		size,
		input.mimeType,
		expiresAt,
		input.kind,
		input.status,
		input.displayName,
		"",
		input.metadataJSON,
		input.retentionState,
		"",
		nil,
		deletedAt,
		input.createdAt,
		input.updatedAt,
	}
}

func analysisRunDriverRow(createdAt, expiresAt time.Time) []driver.Value {
	return analysisRunDriverRowWithState(createdAt, expiresAt, AnalysisRunStatusQueued, 2, nil, nil, nil)
}

func analysisRunDriverRowWithState(createdAt, expiresAt time.Time, status string, version int64, startedAt, completedAt, canceledAt *time.Time) []driver.Value {
	return []driver.Value{
		"run-1",
		"web",
		"user-1",
		"",
		"selection-1",
		"transcription",
		status,
		version,
		[]byte(`{"language":"ru"}`),
		[]byte(`{"strategy":"polling"}`),
		"not_required",
		createdAt,
		startedAtValue(startedAt),
		startedAtValue(completedAt),
		startedAtValue(canceledAt),
		expiresAt,
	}
}

func artifactDriverRow(createdAt, expiresAt time.Time) []driver.Value {
	return []driver.Value{
		"artifact-1",
		"web",
		"user-1",
		"",
		"run-1",
		"transcript",
		ArtifactStatusAvailable,
		"artifacts/run-1/transcript.md",
		"text/markdown",
		"sha256:222",
		int64(128),
		"owner",
		[]byte(`{"available":true}`),
		RetentionStateActive,
		"",
		createdAt,
		expiresAt,
		nil,
	}
}

func collectionDriverRow(id, name, status string, version int64, now time.Time) []driver.Value {
	return []driver.Value{id, "web", "user-1", "", CollectionKindUser, name, status, version, now, now, nil, nil}
}

func selectionDriverRow(id, sourceCollectionID string, now time.Time) []driver.Value {
	return []driver.Value{id, "web", "user-1", "", SelectionStatusSealed, sourceCollectionID, []byte(`{"duplicate_policy":"keep_all"}`), "tester", []byte(`[]`), now, now}
}

func selectionItemDriverRow(createdAt time.Time) []driver.Value {
	return []driver.Value{
		"selection-item-1",
		int64(0),
		"media-1",
		"text",
		[]byte(`{"source_id":"source-1","origin_type":"object","object_key":"sources/source-1/source.txt","checksum":"sha256:111","size_bytes":42,"mime_type":"text/plain"}`),
		"source.txt",
		MediaStatusReady,
		[]byte(`{"tag":"one"}`),
		[]byte(`{"state":"active"}`),
		[]byte(`[]`),
	}
}

func startedAtValue(ts *time.Time) any {
	if ts == nil {
		return nil
	}
	return *ts
}

func compactSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func expectExecArgs(expected map[int]any) func([]driver.NamedValue) error {
	return func(args []driver.NamedValue) error {
		for index, want := range expected {
			if index < 0 || index >= len(args) {
				return fmt.Errorf("exec arg %d missing: got %d args", index, len(args))
			}
			got := args[index].Value
			if fmt.Sprint(got) != fmt.Sprint(want) {
				return fmt.Errorf("exec arg %d = %v, want %v", index, got, want)
			}
		}
		return nil
	}
}

func mediaItemColumns() []string {
	return []string{
		"id", "owner_type", "owner_id", "tenant_id", "source_id", "origin_type", "external_uri", "object_key", "text_ref", "checksum", "size_bytes", "mime_type", "source_expires_at", "kind", "status", "display_name", "adapter_origin", "metadata", "retention_state", "retention_policy_id", "media_expires_at", "deleted_at", "created_at", "updated_at",
	}
}

func collectionColumns() []string {
	return []string{"id", "owner_type", "owner_id", "tenant_id", "kind", "name", "status", "version", "created_at", "updated_at", "archived_at", "deleted_at"}
}

func collectionItemColumns() []string {
	return []string{"media_item_id", "position", "added_by", "added_at", "removed_at"}
}

func selectionColumns() []string {
	return []string{"id", "owner_type", "owner_id", "tenant_id", "status", "source_collection_id", "option_snapshot", "created_by", "diagnostics", "created_at", "sealed_at"}
}

func selectionItemColumns() []string {
	return []string{"id", "position", "media_item_id", "kind", "source_snapshot", "display_name", "status_at_selection", "metadata_snapshot", "retention_snapshot", "diagnostics"}
}

func analysisRunColumns() []string {
	return []string{"id", "owner_type", "owner_id", "tenant_id", "selection_id", "run_type", "status", "version", "params", "delivery", "evidence_gate_state", "created_at", "started_at", "completed_at", "canceled_at", "expires_at"}
}

func runEventColumns() []string {
	return []string{"id", "analysis_run_id", "event_type", "version", "payload", "status", "created_at"}
}

func artifactColumns() []string {
	return []string{"id", "owner_type", "owner_id", "tenant_id", "analysis_run_id", "kind", "status", "object_key", "content_type", "checksum", "size_bytes", "visibility", "preview", "retention_state", "retention_policy_id", "created_at", "expires_at", "deleted_at"}
}

func diagnosticColumns() []string {
	return []string{"id", "owner_type", "owner_id", "tenant_id", "subject_type", "subject_id", "severity", "code", "message", "context", "safe_adapter_context", "correlation_id", "remediation_hint", "created_at"}
}

func analysisRunTaskColumns() []string {
	return []string{"id", "analysis_run_id", "worker_kind", "task_type", "status", "attempt_no", "lease_owner", "claimed_at", "heartbeat_at", "finalized_at", "created_at"}
}

func analysisRunQueueColumns() []string {
	return []string{"analysis_run_id", "run_type", "worker_kind", "task_type", "status", "version", "attempt_no", "created_at"}
}

func orphanColumns() []string {
	return []string{"subject_type", "subject_id", "owner_type", "owner_id", "tenant_id", "bucket", "object_key", "reason"}
}

func expiresAt(base time.Time) time.Time {
	return base.Add(30 * time.Minute)
}
