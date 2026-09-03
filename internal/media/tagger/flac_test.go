package tagger

import (
	"bytes"
	"context"
	"testing"
)

func TestTagFLAC_NonFLAC(t *testing.T) {
	input := []byte("RIFF1234WAVEfmt ")
	in := bytes.NewReader(input)
	var out bytes.Buffer

	meta := &Metadata{Title: "Test Title"}
	err := TagFLAC(context.Background(), in, &out, meta, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.Equal(out.Bytes(), input) {
		t.Fatalf("expected non-FLAC data to be copied verbatim")
	}
}

func TestTagFLAC_FLACStream(t *testing.T) {
	// Minimal valid FLAC stream: "fLaC" + STREAMINFO block (34 bytes) + 1 frame
	flacHeader := []byte("fLaC")
	// Block header: last=1, type=0 (STREAMINFO), length=34
	streamInfoHead := []byte{0x80, 0x00, 0x00, 34}
	streamInfoData := make([]byte, 34)
	dummyAudioFrame := []byte{0xFF, 0xF8, 0x00, 0x00} // dummy frame sync

	var rawFLAC bytes.Buffer
	rawFLAC.Write(flacHeader)
	rawFLAC.Write(streamInfoHead)
	rawFLAC.Write(streamInfoData)
	rawFLAC.Write(dummyAudioFrame)

	var taggedOut bytes.Buffer
	meta := &Metadata{
		Title:       "Test Track",
		Artist:      "Test Artist",
		Album:       "Test Album",
		TrackNumber: 1,
		Date:        "2026-09-03",
		ISRC:        "TEST12345678",
	}

	err := TagFLAC(context.Background(), &rawFLAC, &taggedOut, meta, nil)
	if err != nil {
		t.Fatalf("TagFLAC failed: %v", err)
	}

	outBytes := taggedOut.Bytes()
	if !bytes.HasPrefix(outBytes, []byte("fLaC")) {
		t.Fatalf("tagged output must start with fLaC")
	}

	// Verify that Vorbis Comment is present
	if !bytes.Contains(outBytes, []byte("TITLE=Test Track")) {
		t.Errorf("missing TITLE tag in tagged FLAC")
	}
	if !bytes.Contains(outBytes, []byte("ARTIST=Test Artist")) {
		t.Errorf("missing ARTIST tag in tagged FLAC")
	}
	if !bytes.Contains(outBytes, []byte("ISRC=TEST12345678")) {
		t.Errorf("missing ISRC tag in tagged FLAC")
	}
	if !bytes.Contains(outBytes, []byte("YEAR=2026")) {
		t.Errorf("missing auto-derived YEAR tag in tagged FLAC")
	}

	// Verify audio frames were preserved at the end
	if !bytes.HasSuffix(outBytes, dummyAudioFrame) {
		t.Errorf("audio frame was corrupted or not preserved")
	}
}
