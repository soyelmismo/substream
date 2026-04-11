package specid

// this package is at such a high level in the hierarchy because
// it's used by both `server/db` (for now) and `server/ctrlsubsonic`

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrBadSeparator = errors.New("bad separator")
	ErrNotAnInt     = errors.New("not an int")
	ErrBadPrefix    = errors.New("bad prefix")
	ErrBadJSON      = errors.New("bad JSON")
	ErrBadURN       = errors.New("bad URN format")
)

type IDT string

const (
	Artist               IDT = "ar"
	Album                IDT = "al"
	Track                IDT = "tr"
	Podcast              IDT = "pd"
	PodcastEpisode       IDT = "pe"
	InternetRadioStation IDT = "ir"
	Playlist             IDT = "pl"
	separator                = "-"
	defaultProvider          = "td" // tidal
)

// ID represents a resource identifier using URN format: [provider]:[type]:[id]
// Examples: td:tr:12345 (Tidal Track), td:al:67890 (Tidal Album)
// Legacy format supported for backwards compatibility: tr-12345
type ID struct {
	URI string // Primary storage: provider:type:rawID (e.g., "td:tr:12345")
}

// Provider returns the provider code from the URI (e.g., "td" for Tidal)
func (i ID) Provider() string {
	parts := strings.Split(i.URI, ":")
	if len(parts) >= 3 {
		return parts[0]
	}
	return defaultProvider
}

// Type returns the resource type from the URI (e.g., "tr", "al", "ar")
func (i ID) Type() IDT {
	parts := strings.Split(i.URI, ":")
	if len(parts) >= 3 {
		return IDT(parts[1])
	}
	// Fallback: try legacy format
	partType, _, _ := strings.Cut(i.URI, separator)
	return IDT(partType)
}

// RawID returns the raw identifier string from the URI
func (i ID) RawID() string {
	parts := strings.Split(i.URI, ":")
	if len(parts) >= 3 {
		return parts[2]
	}
	// Fallback: try legacy format
	_, partValue, _ := strings.Cut(i.URI, separator)
	return partValue
}

// Value returns the integer ID value (for backwards compatibility with numeric IDs)
func (i ID) Value() int {
	raw := i.RawID()
	if raw == "" {
		return 0
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return val
}

// New parses an ID string, supporting both URN format (td:tr:123) and legacy format (tr-123)
func New(in string) (ID, error) {
	if in == "" {
		return ID{}, ErrBadJSON
	}

	// Check for URN format: provider:type:id (e.g., "td:tr:12345")
	if strings.Count(in, ":") >= 2 {
		parts := strings.Split(in, ":")
		if len(parts) >= 3 {
			provider := parts[0]
			idType := IDT(parts[1])
			rawID := parts[2]

			// Validate provider is not empty
			if provider == "" {
				return ID{}, ErrBadURN
			}

			// Validate type
			switch idType {
			case Artist, Album, Track, Podcast, PodcastEpisode, InternetRadioStation, Playlist:
				return ID{URI: fmt.Sprintf("%s:%s:%s", provider, idType, rawID)}, nil
			default:
				return ID{}, fmt.Errorf("%q: %w", idType, ErrBadPrefix)
			}
		}
	}

	// Legacy format support: type-value (e.g., "tr-12345", "pl-abc123")
	partType, partValue, ok := strings.Cut(in, separator)
	if !ok {
		return ID{}, ErrBadSeparator
	}

	switch IDT(partType) {
	case Playlist:
		// Playlists use string IDs
		return ID{URI: fmt.Sprintf("%s:%s:%s", defaultProvider, Playlist, partValue)}, nil
	}

	// For other types, validate that partValue is numeric
	val, err := strconv.Atoi(partValue)
	if err != nil {
		return ID{}, fmt.Errorf("%q: %w", partValue, ErrNotAnInt)
	}

	switch IDT(partType) {
	case Artist:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, Artist, val)}, nil
	case Album:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, Album, val)}, nil
	case Track:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, Track, val)}, nil
	case Podcast:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, Podcast, val)}, nil
	case PodcastEpisode:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, PodcastEpisode, val)}, nil
	case InternetRadioStation:
		return ID{URI: fmt.Sprintf("%s:%s:%d", defaultProvider, InternetRadioStation, val)}, nil
	default:
		return ID{}, fmt.Errorf("%q: %w", partType, ErrBadPrefix)
	}
}

// String returns the URN format string representation
func (i ID) String() string {
	if i.URI != "" {
		return i.URI
	}
	return "-1"
}

// LegacyString returns the legacy format for backwards compatibility (used internally)
func (i ID) LegacyString() string {
	return fmt.Sprintf("%s%s%s", i.Type(), separator, i.RawID())
}

func (i ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

func (i *ID) UnmarshalJSON(data []byte) error {
	if len(data) <= 2 {
		return fmt.Errorf("too short: %w", ErrBadJSON)
	}
	id, err := New(string(data[1 : len(data)-1])) // Strip quotes
	if err == nil {
		*i = id
	}
	return err
}

func (i ID) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

// MustParse parses an ID string and panics on error (for testing/known-good inputs)
func MustParse(in string) ID {
	id, err := New(in)
	if err != nil {
		panic(fmt.Sprintf("failed to parse ID %q: %v", in, err))
	}
	return id
}
