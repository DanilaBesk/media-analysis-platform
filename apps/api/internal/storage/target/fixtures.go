package target

import "time"

func DeterministicSeedFixtures() SeedFixtures {
	createdAt := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	return SeedFixtures{
		ChannelAccount: ChannelAccountRecord{
			ID:                 "00000000-0000-4000-8000-000000000001",
			Channel:            "local",
			ExternalAccountRef: "single-user",
			DisplayName:        "Single user",
			Status:             "active",
			MetadataJSON:       []byte(`{"seed":"target-storage"}`),
			CreatedAt:          createdAt,
			UpdatedAt:          createdAt,
		},
		InboxCollection: CollectionRecord{
			ID:               "00000000-0000-4000-8000-000000000101",
			ChannelAccountID: "00000000-0000-4000-8000-000000000001",
			Kind:             "inbox",
			Name:             "Inbox",
			Status:           "active",
			Version:          1,
			CreatedAt:        createdAt,
			UpdatedAt:        createdAt,
		},
	}
}
