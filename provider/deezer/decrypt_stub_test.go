//go:build !deezer_native

package deezer

import (
	"bytes"
	"testing"
)

func TestStubStreamFilterDisabledByDefault(t *testing.T) {
	var dst bytes.Buffer
	src := bytes.NewReader([]byte("dummy_stream_content_buffer"))
	err := DecryptStream(&dst, src, "3135556", "")
	if err == nil {
		t.Fatalf("expected error in stub mode, got nil")
	}
	if err != ErrStreamFilterNotConfigured {
		t.Errorf("unexpected error: %v", err)
	}

	key, err := DeriveStreamKey("3135556", "")
	if err == nil || key != nil {
		t.Fatalf("expected error deriving key in stub mode, got key=%v err=%v", key, err)
	}
}
