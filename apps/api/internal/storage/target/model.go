package target

import "time"

type ChannelAccountRecord struct {
	ID                 string
	Channel            string
	ExternalAccountRef string
	DisplayName        string
	Status             string
	MetadataJSON       []byte
	CreatedAt          time.Time
	UpdatedAt          time.Time
	LastSeenAt         *time.Time
	DisabledAt         *time.Time
}

type OperationRequestRecord struct {
	ID               string
	ChannelAccountID string
	OperationType    string
	IdempotencyKey   string
	RequestHash      string
	Status           string
	TargetType       string
	TargetID         string
	ErrorCode        string
	MetadataJSON     []byte
	CreatedAt        time.Time
	CompletedAt      *time.Time
}

type StoredObjectRecord struct {
	ID             string
	Bucket         string
	ObjectKey      string
	ContentType    string
	SizeBytes      int64
	Checksum       string
	StorageStatus  string
	RetentionState string
	CreatedAt      time.Time
	ExpiresAt      *time.Time
	DeletedAt      *time.Time
}

type MediaAssetRecord struct {
	ID               string
	ChannelAccountID string
	StoredObjectID   string
	OriginType       string
	OriginRef        string
	Kind             string
	DisplayName      string
	Status           string
	MetadataJSON     []byte
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DeletedAt        *time.Time
}

type CollectionRecord struct {
	ID               string
	ChannelAccountID string
	Kind             string
	Name             string
	Status           string
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ArchivedAt       *time.Time
	DeletedAt        *time.Time
}

type CollectionItemRecord struct {
	ID              string
	CollectionID    string
	MediaAssetID    string
	Position        int
	AddedViaChannel string
	AddedAt         time.Time
	RemovedAt       *time.Time
	MediaAsset      *MediaAssetRecord
}

type SelectionSnapshotRecord struct {
	ID                 string
	ChannelAccountID   string
	SourceCollectionID string
	Status             string
	OptionSnapshotJSON []byte
	DiagnosticsJSON    []byte
	CreatedViaChannel  string
	CreatedAt          time.Time
	SealedAt           time.Time
}

type SelectionSnapshotItemRecord struct {
	ID                  string
	SelectionSnapshotID string
	Position            int
	MediaAssetID        string
	Kind                string
	DisplayName         string
	OriginSnapshotJSON  []byte
	StorageSnapshotJSON []byte
	MetadataJSON        []byte
	StatusAtSelection   string
	DiagnosticsJSON     []byte
}

type AnalysisRunRecord struct {
	ID                string
	ChannelAccountID  string
	SelectionSnapshot string
	RunType           string
	Status            string
	Version           int64
	IdempotencyKey    string
	ParamsJSON        []byte
	DeliveryJSON      []byte
	EvidenceGateState string
	CreatedViaChannel string
	CreatedAt         time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
	CancelRequestedAt *time.Time
	CanceledAt        *time.Time
	ExpiresAt         *time.Time
}

type AnalysisRunStepRecord struct {
	ID            string
	AnalysisRunID string
	StepKind      string
	WorkerKind    string
	Status        string
	AttemptNo     int
	LeaseOwner    string
	ClaimedAt     *time.Time
	HeartbeatAt   *time.Time
	FinalizedAt   *time.Time
	MetadataJSON  []byte
	CreatedAt     time.Time
}

type AnalysisRunStepInputRecord struct {
	ID                      string
	AnalysisRunStepID       string
	InputKind               string
	SelectionSnapshotItemID string
	ArtifactID              string
	Position                int
	Required                bool
	MetadataJSON            []byte
	CreatedAt               time.Time
}

type AnalysisRunStepQueueRecord struct {
	AnalysisRunID     string
	RunType           string
	WorkerKind        string
	StepKind          string
	Status            string
	Version           int64
	AttemptNo         int
	AnalysisRunStepID string
	CreatedAt         time.Time
}

type AnalysisRunEventRecord struct {
	ID            string
	AnalysisRunID string
	EventType     string
	Version       int64
	Status        string
	PayloadJSON   []byte
	CreatedAt     time.Time
}

type ArtifactRecord struct {
	ID               string
	ChannelAccountID string
	AnalysisRunID    string
	StoredObjectID   string
	Kind             string
	Status           string
	ContentType      string
	Checksum         string
	SizeBytes        int64
	Visibility       string
	PreviewJSON      []byte
	CreatedAt        time.Time
	ExpiresAt        *time.Time
	DeletedAt        *time.Time
}

type ArtifactSubjectRecord struct {
	ID          string
	ArtifactID  string
	SubjectType string
	SubjectID   string
	SubjectRole string
	CreatedAt   time.Time
}

type DiagnosticRecord struct {
	ID                 string
	ChannelAccountID   string
	SubjectType        string
	SubjectID          string
	Severity           string
	Code               string
	Message            string
	ContextJSON        []byte
	SafeChannelContext []byte
	CorrelationID      string
	RemediationHint    string
	CreatedAt          time.Time
}

type DiagnosticQuery struct {
	ChannelAccountID string
	SubjectType      string
	SubjectID        string
	Severity         string
	Code             string
	CorrelationID    string
}

type ChannelSurfaceRecord struct {
	ID                 string
	ChannelAccountID   string
	Channel            string
	SurfaceType        string
	SurfaceKey         string
	AddressJSON        []byte
	AddressFingerprint string
	DisplayStateJSON   []byte
	LifecycleStatus    string
	Version            int64
	IdempotencyKey     string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	LastRenderedAt     *time.Time
	SupersededAt       *time.Time
	DeletedAt          *time.Time
}

type ChannelSurfaceQuery struct {
	ChannelAccountID string
	SubjectType      string
	SubjectID        string
	LifecycleStatus  string
	ActiveOnly       bool
}

type ChannelSurfaceSubjectRecord struct {
	SurfaceID   string
	SubjectType string
	SubjectID   string
	SubjectRole string
	CreatedAt   time.Time
}

type ChannelSurfaceEventRecord struct {
	ID              string
	SurfaceID       string
	EventType       string
	Reason          string
	PreviousVersion int64
	NextVersion     int64
	ActorType       string
	ActorID         string
	MetadataJSON    []byte
	CreatedAt       time.Time
}

type CreateMediaAssetWithInboxParams struct {
	StoredObject    StoredObjectRecord
	MediaAsset      MediaAssetRecord
	InboxCollection CollectionRecord
	CollectionItem  CollectionItemRecord
}

type UpdateCollectionParams struct {
	ChannelAccountID string
	CollectionID     string
	ExpectedVersion  int64
	Name             string
	Status           string
	UpdatedAt        time.Time
}

type UpdateCollectionItemsParams struct {
	ChannelAccountID string
	CollectionID     string
	ExpectedVersion  int64
	Items            []CollectionItemRecord
	UpdatedAt        time.Time
}

type RemoveCollectionItemParams struct {
	ChannelAccountID string
	CollectionID     string
	MediaAssetID     string
	ExpectedVersion  int64
	RemovedAt        time.Time
}

type UpdateChannelAccountParams struct {
	ID           string
	DisplayName  string
	Status       string
	MetadataJSON []byte
	LastSeenAt   *time.Time
	DisabledAt   *time.Time
	UpdatedAt    time.Time
}

type RecordAnalysisRunProgressParams struct {
	AnalysisRunID     string
	AnalysisRunStepID string
	HeartbeatAt       time.Time
	Event             AnalysisRunEventRecord
}

type FinalizeAnalysisRunStepParams struct {
	AnalysisRunID     string
	AnalysisRunStepID string
	StepStatus        string
	RunStatus         string
	Message           string
	FinalizedAt       time.Time
	Event             AnalysisRunEventRecord
}

type ReplaceChannelSurfaceDisplayStateParams struct {
	SurfaceID        string
	ExpectedVersion  int64
	DisplayStateJSON []byte
	UpdatedAt        time.Time
	Event            ChannelSurfaceEventRecord
}

type AnalysisRunGraph struct {
	Run        AnalysisRunRecord
	Steps      []AnalysisRunStepRecord
	StepInputs []AnalysisRunStepInputRecord
	Event      AnalysisRunEventRecord
}

type SupersedeChannelSurfaceParams struct {
	SurfaceID    string
	SupersededAt time.Time
	Event        ChannelSurfaceEventRecord
}

type SeedFixtures struct {
	ChannelAccount  ChannelAccountRecord
	InboxCollection CollectionRecord
}
