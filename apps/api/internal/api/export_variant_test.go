package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
	targetstore "github.com/danila/media-analysis-platform/apps/api/internal/storage/target"
)

func TestNormalizeExportVariantUsesSupportedAACBitrates(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"youtube_audio", "video_to_audio"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			for _, bitrate := range []int{64, 96, 128, 192, 256, 320} {
				if _, err := normalizeExportVariant(operation, []byte(fmt.Sprintf(`{"audio_bitrate_kbps":%d}`, bitrate))); err != nil {
					t.Fatalf("normalizeExportVariant(%q, %d) error = %v", operation, bitrate, err)
				}
			}
			_, err := normalizeExportVariant(operation, []byte(`{"audio_bitrate_kbps":384}`))
			if !errors.Is(err, storage.ErrContractViolation) {
				t.Fatalf("normalizeExportVariant(%q, 384) error = %v, want ErrContractViolation", operation, err)
			}
		})
	}
}

func TestExportPresentationKindPreservesProfileCompatibility(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		operation string
		profile   string
		want      string
	}{
		{operation: "youtube_audio", profile: exportProfileAudioM4AV1, want: "music"},
		{operation: "youtube_audio", profile: exportProfileAudioM4ALegacy, want: "audio"},
		{operation: "youtube_audio", profile: exportProfileAudioOGGOpusV1, want: "audio"},
		{operation: "video_to_audio", profile: exportProfileAudioM4AV1, want: "audio"},
		{operation: "youtube_video", profile: exportProfileVideoMP4V1, want: "document"},
	} {
		if got := exportPresentationKind(targetstore.ExportJobRecord{Operation: test.operation, OutputProfile: test.profile}); got != test.want {
			t.Fatalf("exportPresentationKind(%q, %q) = %q, want %q", test.operation, test.profile, got, test.want)
		}
	}
}
