package ctrlsubsonic

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestIsExoPlayerClient(t *testing.T) {
	tests := []struct {
		client   string
		expected bool
	}{
		{"Tempus", true},
		{"tempus-android", true},
		{"Symfonium", true},
		{"DSub", true},
		{"ExoPlayerLib/2.18.1", true},
		{"supersonic", false},
		{"substream-web", false},
		{"audinaut", false},
	}

	for _, tt := range tests {
		if got := isExoPlayerClient(tt.client); got != tt.expected {
			t.Errorf("isExoPlayerClient(%q) = %v, expected %v", tt.client, got, tt.expected)
		}
	}
}

func TestDoesntSupportHLS(t *testing.T) {
	tests := []struct {
		client   string
		expected bool
	}{
		{"Tempus", true},
		{"symfonium", true},
		{"psysonic", true},
		{"supersonic", false},
		{"anyother", false},
	}

	for _, tt := range tests {
		if got := doesntSupportHLS(tt.client); got != tt.expected {
			t.Errorf("doesntSupportHLS(%q) = %v, expected %v", tt.client, got, tt.expected)
		}
	}
}

func TestCreateFakeSidx(t *testing.T) {
	segments := []segmentInfo{
		{url: "http://example.com/seg1.m4s", duration: 5.0},
		{url: "http://example.com/seg2.m4s", duration: 5.0},
		{url: "http://example.com/seg3.m4s", duration: 4.5},
	}

	bps := int64(125000)
	firstOffset := uint32(100)

	sidx := createFakeSidx(segments, bps, firstOffset)

	expectedLen := 32 + len(segments)*12
	if len(sidx) != expectedLen {
		t.Fatalf("expected sidx length %d, got %d", expectedLen, len(sidx))
	}

	boxLen := binary.BigEndian.Uint32(sidx[0:4])
	if int(boxLen) != expectedLen {
		t.Errorf("expected box size in header %d, got %d", expectedLen, boxLen)
	}

	if !bytes.Equal(sidx[4:8], []byte("sidx")) {
		t.Errorf("expected fourcc 'sidx', got %q", string(sidx[4:8]))
	}

	refCount := binary.BigEndian.Uint16(sidx[30:32])
	if int(refCount) != len(segments) {
		t.Errorf("expected reference count %d, got %d", len(segments), refCount)
	}
}
