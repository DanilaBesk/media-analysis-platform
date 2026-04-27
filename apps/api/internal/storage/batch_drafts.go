package storage

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

func (r *Repository) CreateBatchDraft(ctx context.Context, req BatchDraftCreate) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	if err := validateBatchDraftOwner(req.Owner); err != nil {
		return BatchDraftRecord{}, err
	}
	draftID := strings.TrimSpace(req.DraftID)
	if draftID == "" {
		draftID = r.nextID()
	}
	now := r.now()
	draft := BatchDraftRecord{
		DraftID:     draftID,
		Version:     1,
		Owner:       normalizeBatchDraftOwner(req.Owner),
		Status:      BatchDraftStatusOpen,
		DisplayName: strings.TrimSpace(req.DisplayName),
		ClientRef:   strings.TrimSpace(req.ClientRef),
		ExpiresAt:   req.ExpiresAt,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items:       []BatchDraftItem{},
	}
	created, err := store.CreateBatchDraft(ctx, draft)
	if err != nil {
		return BatchDraftRecord{}, fmt.Errorf("%w: create batch draft: %v", ErrStorageUnavailable, err)
	}
	return created, nil
}

func (r *Repository) GetBatchDraft(ctx context.Context, draftID string, owner BatchDraftOwner) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	if strings.TrimSpace(draftID) == "" {
		return BatchDraftRecord{}, fmt.Errorf("%w: batch draft id is required", ErrContractViolation)
	}
	if err := validateBatchDraftOwner(owner); err != nil {
		return BatchDraftRecord{}, err
	}
	draft, err := store.GetBatchDraft(ctx, strings.TrimSpace(draftID))
	if err != nil {
		if errorsIsBatchDraftDomain(err) {
			return BatchDraftRecord{}, err
		}
		return BatchDraftRecord{}, fmt.Errorf("%w: get batch draft: %v", ErrStorageUnavailable, err)
	}
	if !sameBatchDraftOwner(draft.Owner, owner) {
		return BatchDraftRecord{}, ErrBatchDraftOwnerMismatch
	}
	return draft, nil
}

func (r *Repository) AddBatchDraftItem(ctx context.Context, req BatchDraftItemMutation) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	req = normalizeBatchDraftItemMutation(req)
	if err := validateBatchDraftItemMutation(req); err != nil {
		return BatchDraftRecord{}, err
	}
	if requiresSourceObject(req.Source.SourceKind) && req.Source.ObjectBody != nil {
		draft, err := store.GetBatchDraft(ctx, req.DraftID)
		if err != nil {
			if errorsIsBatchDraftDomain(err) {
				return BatchDraftRecord{}, err
			}
			return BatchDraftRecord{}, fmt.Errorf("%w: get batch draft before source upload: %v", ErrStorageUnavailable, err)
		}
		if err := checkBatchDraftMutation(draft, req.Owner, req.ExpectedVersion); err != nil {
			return BatchDraftRecord{}, err
		}
	}
	now := r.now()
	itemID := r.nextID()
	sourceID := r.nextID()
	item := BatchDraftItem{
		ItemID:      itemID,
		SourceID:    sourceID,
		SourceLabel: batchDraftSourceLabel(itemID),
		Source:      req.Source,
		CreatedAt:   now,
	}
	if requiresSourceObject(req.Source.SourceKind) && req.Source.ObjectBody != nil {
		if err := r.objects.PutObject(ctx, SourcesBucket, req.Source.UploadedSourceRef, req.Source.MIMEType, req.Source.ObjectBody); err != nil {
			return BatchDraftRecord{}, fmt.Errorf("%w: persist batch draft source object: %v", ErrStorageUnavailable, err)
		}
		item.Source.ObjectBody = nil
	}
	draft, err := store.AddBatchDraftItem(ctx, req, item)
	if err != nil {
		if errorsIsBatchDraftDomain(err) {
			return BatchDraftRecord{}, err
		}
		return BatchDraftRecord{}, fmt.Errorf("%w: add batch draft item: %v", ErrStorageUnavailable, err)
	}
	return draft, nil
}

func (r *Repository) RemoveBatchDraftItem(ctx context.Context, req BatchDraftItemRemove) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	req.DraftID = strings.TrimSpace(req.DraftID)
	req.ItemID = strings.TrimSpace(req.ItemID)
	req.Owner = normalizeBatchDraftOwner(req.Owner)
	if req.DraftID == "" || req.ItemID == "" || req.ExpectedVersion <= 0 {
		return BatchDraftRecord{}, fmt.Errorf("%w: draft id, item id, and expected version are required", ErrContractViolation)
	}
	if err := validateBatchDraftOwner(req.Owner); err != nil {
		return BatchDraftRecord{}, err
	}
	draft, err := store.RemoveBatchDraftItem(ctx, req)
	if err != nil {
		if errorsIsBatchDraftDomain(err) {
			return BatchDraftRecord{}, err
		}
		return BatchDraftRecord{}, fmt.Errorf("%w: remove batch draft item: %v", ErrStorageUnavailable, err)
	}
	return draft, nil
}

func (r *Repository) ClearBatchDraft(ctx context.Context, req BatchDraftMutation) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	req.DraftID = strings.TrimSpace(req.DraftID)
	req.Owner = normalizeBatchDraftOwner(req.Owner)
	if req.DraftID == "" || req.ExpectedVersion <= 0 {
		return BatchDraftRecord{}, fmt.Errorf("%w: draft id and expected version are required", ErrContractViolation)
	}
	if err := validateBatchDraftOwner(req.Owner); err != nil {
		return BatchDraftRecord{}, err
	}
	draft, err := store.ClearBatchDraft(ctx, req)
	if err != nil {
		if errorsIsBatchDraftDomain(err) {
			return BatchDraftRecord{}, err
		}
		return BatchDraftRecord{}, fmt.Errorf("%w: clear batch draft: %v", ErrStorageUnavailable, err)
	}
	return draft, nil
}

func (r *Repository) SubmitBatchDraftGraph(ctx context.Context, req BatchDraftGraphSubmission) (BatchDraftRecord, error) {
	store, err := r.batchDraftStore()
	if err != nil {
		return BatchDraftRecord{}, err
	}
	req.DraftID = strings.TrimSpace(req.DraftID)
	req.Owner = normalizeBatchDraftOwner(req.Owner)
	if req.DraftID == "" || req.ExpectedVersion <= 0 {
		return BatchDraftRecord{}, fmt.Errorf("%w: draft id and expected version are required", ErrContractViolation)
	}
	if err := validateBatchDraftOwner(req.Owner); err != nil {
		return BatchDraftRecord{}, err
	}
	if len(req.Jobs) > 0 {
		req.Submission = *ensureSubmissionDefaults(&req.Submission, r.now)
		if err := validateSubmission(req.Submission); err != nil {
			return BatchDraftRecord{}, err
		}
		normalizedSources := make([]SourceRecord, 0, len(req.Sources))
		for _, source := range req.Sources {
			source = ensureSourceDefaults(source, r.now)
			if err := validateSource(source); err != nil {
				return BatchDraftRecord{}, err
			}
			if requiresSourceObject(source.SourceKind) && !source.ObjectAlreadyPersisted {
				if err := r.objects.PutObject(ctx, SourcesBucket, source.ObjectKey, source.MIMEType, source.ObjectBody); err != nil {
					return BatchDraftRecord{}, fmt.Errorf("%w: persist source object: %v", ErrStorageUnavailable, err)
				}
			}
			normalizedSources = append(normalizedSources, source.withoutObjectBody())
		}
		req.Sources = normalizedSources
		for idx := range req.SourceSets {
			req.SourceSets[idx] = ensureSourceSetDefaults(req.SourceSets[idx], r.now)
			if err := validateSourceSet(req.SourceSets[idx], req.Sources); err != nil {
				return BatchDraftRecord{}, err
			}
		}
		for idx := range req.Jobs {
			req.Jobs[idx] = ensureJobDefaults(req.Jobs[idx], r.now)
			var matchedSourceSet *SourceSetRecord
			for sourceSetIdx := range req.SourceSets {
				if req.SourceSets[sourceSetIdx].ID == req.Jobs[idx].SourceSetID {
					matchedSourceSet = &req.SourceSets[sourceSetIdx]
					break
				}
			}
			if matchedSourceSet == nil {
				return BatchDraftRecord{}, fmt.Errorf("%w: job source_set_id must reference one of the persisted source sets", ErrContractViolation)
			}
			if err := validateJob(req.Jobs[idx], *matchedSourceSet); err != nil {
				return BatchDraftRecord{}, err
			}
		}
	}
	draft, err := store.SubmitBatchDraftGraph(ctx, req)
	if err != nil {
		if errorsIsBatchDraftDomain(err) {
			return BatchDraftRecord{}, err
		}
		return BatchDraftRecord{}, fmt.Errorf("%w: submit batch draft graph: %v", ErrStorageUnavailable, err)
	}
	return draft, nil
}

func (r *Repository) batchDraftStore() (batchDraftStateStore, error) {
	store, ok := r.state.(batchDraftStateStore)
	if !ok {
		return nil, fmt.Errorf("%w: batch draft storage is not supported by the configured state store", ErrStorageUnavailable)
	}
	return store, nil
}

func normalizeBatchDraftItemMutation(req BatchDraftItemMutation) BatchDraftItemMutation {
	req.DraftID = strings.TrimSpace(req.DraftID)
	req.Owner = normalizeBatchDraftOwner(req.Owner)
	req.Source.SourceKind = strings.TrimSpace(req.Source.SourceKind)
	req.Source.UploadedSourceRef = strings.TrimSpace(req.Source.UploadedSourceRef)
	req.Source.URL = strings.TrimSpace(req.Source.URL)
	req.Source.DisplayName = strings.TrimSpace(req.Source.DisplayName)
	req.Source.OriginalFilename = strings.TrimSpace(req.Source.OriginalFilename)
	req.Source.MIMEType = strings.TrimSpace(req.Source.MIMEType)
	return req
}

func validateBatchDraftItemMutation(req BatchDraftItemMutation) error {
	if req.DraftID == "" || req.ExpectedVersion <= 0 {
		return fmt.Errorf("%w: draft id and expected version are required", ErrContractViolation)
	}
	if err := validateBatchDraftOwner(req.Owner); err != nil {
		return err
	}
	switch req.Source.SourceKind {
	case SourceKindYouTubeURL, "external_url":
		if req.Source.URL == "" {
			return fmt.Errorf("%w: url draft item requires url", ErrContractViolation)
		}
		if req.Source.UploadedSourceRef != "" || req.Source.ObjectBody != nil || req.Source.MIMEType != "" || req.Source.SizeBytes != 0 || req.Source.OriginalFilename != "" {
			return fmt.Errorf("%w: url draft item must not include upload metadata", ErrContractViolation)
		}
	case SourceKindUploadedFile, SourceKindTelegramUpload:
		if req.Source.UploadedSourceRef == "" {
			return fmt.Errorf("%w: uploaded draft item requires uploaded source ref", ErrContractViolation)
		}
		if !strings.HasPrefix(req.Source.UploadedSourceRef, "sources/batch-drafts/"+req.DraftID+"/uploads/") {
			return fmt.Errorf("%w: uploaded draft item requires API-owned uploaded source ref", ErrContractViolation)
		}
		if req.Source.ObjectBody == nil {
			return fmt.Errorf("%w: uploaded draft item requires object payload", ErrContractViolation)
		}
		if len(req.Source.ObjectBody) == 0 {
			return fmt.Errorf("%w: uploaded draft item object payload must be non-empty", ErrContractViolation)
		}
		if req.Source.MIMEType == "" {
			return fmt.Errorf("%w: uploaded draft item requires content type", ErrContractViolation)
		}
		if req.Source.SizeBytes <= 0 {
			return fmt.Errorf("%w: uploaded draft item requires size_bytes", ErrContractViolation)
		}
		if req.Source.SizeBytes != int64(len(req.Source.ObjectBody)) {
			return fmt.Errorf("%w: uploaded draft item size_bytes must match object payload", ErrContractViolation)
		}
		if req.Source.OriginalFilename == "" {
			return fmt.Errorf("%w: uploaded draft item requires original filename", ErrContractViolation)
		}
	default:
		return fmt.Errorf("%w: unsupported draft item source kind %q", ErrContractViolation, req.Source.SourceKind)
	}
	return nil
}

func validateBatchDraftOwner(owner BatchDraftOwner) error {
	owner = normalizeBatchDraftOwner(owner)
	if owner.OwnerType != "telegram" || owner.TelegramChatID == "" || owner.TelegramUserID == "" {
		return fmt.Errorf("%w: telegram owner scope requires owner_type, telegram_chat_id, and telegram_user_id", ErrContractViolation)
	}
	return nil
}

func normalizeBatchDraftOwner(owner BatchDraftOwner) BatchDraftOwner {
	return BatchDraftOwner{
		OwnerType:      strings.TrimSpace(owner.OwnerType),
		TelegramChatID: strings.TrimSpace(owner.TelegramChatID),
		TelegramUserID: strings.TrimSpace(owner.TelegramUserID),
	}
}

func sameBatchDraftOwner(left, right BatchDraftOwner) bool {
	left = normalizeBatchDraftOwner(left)
	right = normalizeBatchDraftOwner(right)
	return left.OwnerType == right.OwnerType && left.TelegramChatID == right.TelegramChatID && left.TelegramUserID == right.TelegramUserID
}

func batchDraftSourceLabel(itemID string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(itemID)))
	return "item_" + hex.EncodeToString(sum[:])[:16]
}

func errorsIsBatchDraftDomain(err error) bool {
	return errors.Is(err, ErrBatchDraftNotFound) ||
		errors.Is(err, ErrBatchDraftOwnerMismatch) ||
		errors.Is(err, ErrBatchDraftVersionConflict) ||
		errors.Is(err, ErrBatchDraftSubmitted) ||
		errors.Is(err, ErrBatchDraftExpired) ||
		errors.Is(err, ErrBatchDraftCanceled) ||
		errors.Is(err, ErrBatchDraftEmpty)
}

func batchDraftTerminalError(draft BatchDraftRecord) error {
	switch draft.Status {
	case BatchDraftStatusOpen:
		return nil
	case BatchDraftStatusSubmitted:
		return ErrBatchDraftSubmitted
	case BatchDraftStatusExpired:
		return ErrBatchDraftExpired
	case BatchDraftStatusCanceled:
		return ErrBatchDraftCanceled
	default:
		return fmt.Errorf("%w: unsupported batch draft status %q", ErrContractViolation, draft.Status)
	}
}

func checkBatchDraftMutation(draft BatchDraftRecord, owner BatchDraftOwner, expectedVersion int64) error {
	if !sameBatchDraftOwner(draft.Owner, owner) {
		return ErrBatchDraftOwnerMismatch
	}
	if err := batchDraftTerminalError(draft); err != nil {
		return err
	}
	if draft.Version != expectedVersion {
		return ErrBatchDraftVersionConflict
	}
	return nil
}
