package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApiStorageBatchDraftMutationsEnforceOwnerVersionAndTerminalStatus(t *testing.T) {
	t.Parallel()

	state := newMemoryStateStore()
	now := func() time.Time { return time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC) }
	repo, err := NewRepository(state, newFakeObjectStore(), WithClock(now))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	owner := BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"}
	draft, err := repo.CreateBatchDraft(context.Background(), BatchDraftCreate{
		Owner:       owner,
		DisplayName: "Telegram batch",
		ClientRef:   "client-1",
	})
	if err != nil {
		t.Fatalf("CreateBatchDraft() error = %v", err)
	}
	if draft.Version != 1 || draft.Status != BatchDraftStatusOpen {
		t.Fatalf("created draft = %#v, want version=1 status=open", draft)
	}

	draft, err = repo.AddBatchDraftItem(context.Background(), BatchDraftItemMutation{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: draft.Version,
		Source: BatchDraftItemSource{
			SourceKind:  SourceKindYouTubeURL,
			URL:         "https://youtu.be/video",
			DisplayName: "Video",
		},
	})
	if err != nil {
		t.Fatalf("AddBatchDraftItem() error = %v", err)
	}
	if draft.Version != 2 || len(draft.Items) != 1 || draft.Items[0].Position != 0 {
		t.Fatalf("draft after add = %#v, want version=2 and one ordered item", draft)
	}

	_, err = repo.AddBatchDraftItem(context.Background(), BatchDraftItemMutation{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: 1,
		Source: BatchDraftItemSource{
			SourceKind:  SourceKindYouTubeURL,
			URL:         "https://youtu.be/stale",
			DisplayName: "Stale",
		},
	})
	if !errors.Is(err, ErrBatchDraftVersionConflict) {
		t.Fatalf("stale add error = %v, want ErrBatchDraftVersionConflict", err)
	}

	_, err = repo.GetBatchDraft(context.Background(), draft.DraftID, BatchDraftOwner{
		OwnerType:      "telegram",
		TelegramChatID: "chat-1",
		TelegramUserID: "other-user",
	})
	if !errors.Is(err, ErrBatchDraftOwnerMismatch) {
		t.Fatalf("wrong owner get error = %v, want ErrBatchDraftOwnerMismatch", err)
	}

	rootJob := JobRecord{
		ID:          "11111111-1111-1111-1111-111111111111",
		RootJobID:   "11111111-1111-1111-1111-111111111111",
		SourceSetID: "33333333-3333-3333-3333-333333333333",
		JobType:     "transcription",
		Status:      "queued",
		Delivery:    Delivery{Strategy: DeliveryStrategyPolling},
	}
	submitted, err := repo.SubmitBatchDraftGraph(context.Background(), BatchDraftGraphSubmission{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: draft.Version,
		Submission: JobSubmission{
			ID:                 "22222222-2222-2222-2222-222222222222",
			SubmissionKind:     "transcription_batch",
			IdempotencyKey:     "draft:" + draft.DraftID,
			RequestFingerprint: "fingerprint",
		},
		Sources: []SourceRecord{{
			ID:          "44444444-4444-4444-4444-444444444444",
			SourceKind:  SourceKindYouTubeURL,
			DisplayName: "Video",
			SourceURL:   "https://youtu.be/video",
		}},
		SourceSets: []SourceSetRecord{{
			ID:        rootJob.SourceSetID,
			InputKind: SourceSetInputBatchTranscription,
			Items: []SourceSetItem{{
				Position:           0,
				SourceID:           "44444444-4444-4444-4444-444444444444",
				SourceLabel:        draft.Items[0].SourceLabel,
				SourceLabelVersion: "batch-transcription.v1",
			}},
		}},
		Jobs: []JobRecord{rootJob},
	})
	if err != nil {
		t.Fatalf("SubmitBatchDraftGraph() error = %v", err)
	}
	if submitted.Status != BatchDraftStatusSubmitted || submitted.SubmittedRootJobID != rootJob.ID {
		t.Fatalf("submitted draft = %#v, want submitted root %q", submitted, rootJob.ID)
	}

	replayed, err := repo.SubmitBatchDraftGraph(context.Background(), BatchDraftGraphSubmission{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: draft.Version,
	})
	if err != nil {
		t.Fatalf("duplicate SubmitBatchDraftGraph() error = %v", err)
	}
	if replayed.SubmittedRootJobID != rootJob.ID {
		t.Fatalf("duplicate submit root = %q, want %q", replayed.SubmittedRootJobID, rootJob.ID)
	}

	_, err = repo.RemoveBatchDraftItem(context.Background(), BatchDraftItemRemove{
		DraftID:         draft.DraftID,
		ItemID:          draft.Items[0].ItemID,
		Owner:           owner,
		ExpectedVersion: submitted.Version,
	})
	if !errors.Is(err, ErrBatchDraftSubmitted) {
		t.Fatalf("remove after submit error = %v, want ErrBatchDraftSubmitted", err)
	}

	_, err = repo.ClearBatchDraft(context.Background(), BatchDraftMutation{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: submitted.Version,
	})
	if !errors.Is(err, ErrBatchDraftSubmitted) {
		t.Fatalf("clear after submit error = %v, want ErrBatchDraftSubmitted", err)
	}
}

func TestApiStorageBatchDraftSubmitRejectsEmptyOpenDraft(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"}
	draft, err := repo.CreateBatchDraft(context.Background(), BatchDraftCreate{Owner: owner})
	if err != nil {
		t.Fatalf("CreateBatchDraft() error = %v", err)
	}

	_, err = repo.SubmitBatchDraftGraph(context.Background(), BatchDraftGraphSubmission{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: draft.Version,
	})
	if !errors.Is(err, ErrBatchDraftEmpty) {
		t.Fatalf("empty submit error = %v, want ErrBatchDraftEmpty", err)
	}
}

func TestApiStorageBatchDraftAddUploadedItemPersistsObjectAndMetadata(t *testing.T) {
	t.Parallel()

	objects := newFakeObjectStore()
	repo, err := NewRepository(newMemoryStateStore(), objects, WithIDGenerator(sequenceIDs(
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
	)))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"}
	draft, err := repo.CreateBatchDraft(context.Background(), BatchDraftCreate{Owner: owner})
	if err != nil {
		t.Fatalf("CreateBatchDraft() error = %v", err)
	}
	uploadRef := "sources/batch-drafts/" + draft.DraftID + "/uploads/digest/voice.ogg"

	draft, err = repo.AddBatchDraftItem(context.Background(), BatchDraftItemMutation{
		DraftID:         draft.DraftID,
		Owner:           owner,
		ExpectedVersion: draft.Version,
		Source: BatchDraftItemSource{
			SourceKind:        SourceKindTelegramUpload,
			UploadedSourceRef: uploadRef,
			DisplayName:       "Voice",
			OriginalFilename:  "voice.ogg",
			MIMEType:          "audio/ogg",
			SizeBytes:         int64(len("voice-bytes")),
			ObjectBody:        []byte("voice-bytes"),
		},
	})
	if err != nil {
		t.Fatalf("AddBatchDraftItem() error = %v", err)
	}
	if got, want := len(objects.puts), 1; got != want {
		t.Fatalf("object puts = %d, want %d", got, want)
	}
	if put := objects.puts[0]; put.bucket != SourcesBucket || put.objectKey != uploadRef || put.contentType != "audio/ogg" || string(put.body) != "voice-bytes" {
		t.Fatalf("object put = %#v, want source upload body persisted on add", put)
	}
	if got := draft.Items[0].Source; got.UploadedSourceRef != uploadRef || got.MIMEType != "audio/ogg" || got.SizeBytes != int64(len("voice-bytes")) || len(got.ObjectBody) != 0 {
		t.Fatalf("stored source = %#v, want persisted ref metadata without body", got)
	}
}

func TestApiStorageBatchDraftRejectsUploadedItemWithoutAPIOwnedBody(t *testing.T) {
	t.Parallel()

	repo, err := NewRepository(newMemoryStateStore(), newFakeObjectStore())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	owner := BatchDraftOwner{OwnerType: "telegram", TelegramChatID: "chat-1", TelegramUserID: "user-1"}
	draft, err := repo.CreateBatchDraft(context.Background(), BatchDraftCreate{Owner: owner})
	if err != nil {
		t.Fatalf("CreateBatchDraft() error = %v", err)
	}

	tests := []struct {
		name   string
		source BatchDraftItemSource
	}{
		{
			name: "arbitrary ref without body",
			source: BatchDraftItemSource{
				SourceKind:        SourceKindTelegramUpload,
				UploadedSourceRef: "attacker-controlled-ref",
				DisplayName:       "Voice",
			},
		},
		{
			name: "API ref without body",
			source: BatchDraftItemSource{
				SourceKind:        SourceKindUploadedFile,
				UploadedSourceRef: "sources/batch-drafts/" + draft.DraftID + "/uploads/digest/voice.ogg",
				DisplayName:       "Voice",
				OriginalFilename:  "voice.ogg",
				MIMEType:          "audio/ogg",
				SizeBytes:         int64(len("voice-bytes")),
			},
		},
		{
			name: "API ref without content type",
			source: BatchDraftItemSource{
				SourceKind:        SourceKindUploadedFile,
				UploadedSourceRef: "sources/batch-drafts/" + draft.DraftID + "/uploads/digest/voice.ogg",
				DisplayName:       "Voice",
				OriginalFilename:  "voice.ogg",
				SizeBytes:         int64(len("voice-bytes")),
				ObjectBody:        []byte("voice-bytes"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.AddBatchDraftItem(context.Background(), BatchDraftItemMutation{
				DraftID:         draft.DraftID,
				Owner:           owner,
				ExpectedVersion: draft.Version,
				Source:          tt.source,
			})
			if !errors.Is(err, ErrContractViolation) {
				t.Fatalf("AddBatchDraftItem() error = %v, want ErrContractViolation", err)
			}
		})
	}
}

func sequenceIDs(ids ...string) func() string {
	idx := 0
	return func() string {
		if idx >= len(ids) {
			return ids[len(ids)-1]
		}
		id := ids[idx]
		idx++
		return id
	}
}
