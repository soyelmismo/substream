package ctrlsubsonic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// fetchJSON fetches and decodes JSON from a URL using the provided HTTP client.
//
// This function centralizes HTTP requests to external APIs (hot.monochrome.tf) with:
// - Context-aware timeout and cancellation support
// - Proper error handling with wrapped errors for debugging
// - Structured logging with configurable prefix for tracking
// - Response body cleanup via defer
//
// Parameters:
//   - ctx: Context for timeout/cancellation control
//   - client: HTTP client with configured timeouts and connection pooling
//   - url: Target URL for the GET request
//   - logPrefix: Prefix for log messages (e.g., "GENRE", "BROWSE") for filtering
//   - result: Pointer to struct for JSON unmarshaling (must match API response schema)
//
// Returns an error if:
//   - Request creation fails (invalid URL or context)
//   - HTTP request fails (network errors, timeouts)
//   - Server returns non-200 status code
//   - JSON decoding fails (schema mismatch or malformed response)
func fetchJSON(ctx context.Context, client *http.Client, url string, logPrefix string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[%s] Error creating request: %v", logPrefix, err)
		return fmt.Errorf("%s: creating request: %w", logPrefix, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[%s] Error fetching: %v", logPrefix, err)
		return fmt.Errorf("%s: fetching: %w", logPrefix, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[%s] Server returned status %d", logPrefix, resp.StatusCode)
		return fmt.Errorf("%s: server returned status %d", logPrefix, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		log.Printf("[%s] Error decoding response: %v", logPrefix, err)
		return fmt.Errorf("%s: decoding response: %w", logPrefix, err)
	}

	return nil
}
