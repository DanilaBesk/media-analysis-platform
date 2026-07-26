package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/danila/media-analysis-platform/apps/api/internal/storage"
)

func TestNormalizeExportVariantUsesSupportedOpusBitrates(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"youtube_audio", "video_to_audio"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			for _, bitrate := range []int{64, 96, 128, 192, 256} {
				if _, err := normalizeExportVariant(operation, []byte(fmt.Sprintf(`{"audio_bitrate_kbps":%d}`, bitrate))); err != nil {
					t.Fatalf("normalizeExportVariant(%q, %d) error = %v", operation, bitrate, err)
				}
			}
			_, err := normalizeExportVariant(operation, []byte(`{"audio_bitrate_kbps":320}`))
			if !errors.Is(err, storage.ErrContractViolation) {
				t.Fatalf("normalizeExportVariant(%q, 320) error = %v, want ErrContractViolation", operation, err)
			}
		})
	}
}
