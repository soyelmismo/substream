package deezer

import (
	"encoding/json"
	"strings"
)

// isDeezerSubscriptionDegraded analyzes an error or response body to determine
// if a Deezer account has lost its paid/lossless subscription (degraded to FREE),
// or if its session is permanently invalid.
//
// Returns (degraded bool, isFatalAuth bool, reason string):
// - degraded: true if account is FREE or lacks lossless rights (demote tier to FREE)
// - isFatalAuth: true if ARL/token is expired or completely invalid (deactivate account)
// - reason: descriptive reason for logging and database records
func isDeezerSubscriptionDegraded(err error, body string) (degraded bool, isFatalAuth bool, reason string) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	combined := strings.ToLower(errStr + " " + body)

	// Fatal authentication / expired ARL
	if strings.Contains(combined, "invalid arl") ||
		strings.Contains(combined, "session expired") ||
		strings.Contains(combined, "valid_token_required") ||
		strings.Contains(combined, "wrong_credentials") ||
		strings.Contains(combined, "auth_error") {
		return false, true, "session or ARL expired"
	}

	// Media CDN error codes
	if body != "" {
		var mediaResp MediaGetURLResponse
		if jsonErr := json.Unmarshal([]byte(body), &mediaResp); jsonErr == nil {
			for _, e := range mediaResp.Errors {
				if e.Code == 2000 || strings.Contains(strings.ToLower(e.Message), "unauthorized") {
					return true, false, "media error 2000: unauthorized for requested format"
				}
				if e.Code == 1000 {
					return false, true, "media error 1000: license token expired"
				}
			}
			for _, d := range mediaResp.Data {
				for _, e := range d.Errors {
					if e.Code == 2000 || strings.Contains(strings.ToLower(e.Message), "unauthorized") || strings.Contains(strings.ToLower(e.Message), "not allowed") {
						return true, false, "track media error 2000: format not allowed in subscription"
					}
				}
			}
		}
	}

	// Explicit degraded indicators
	if strings.Contains(combined, "weblossless=false") ||
		strings.Contains(combined, "not entitled") ||
		strings.Contains(combined, "subscription degraded") ||
		strings.Contains(combined, "lossless expired") ||
		strings.Contains(combined, "quality not allowed") {
		return true, false, "subscription lacks lossless streaming rights"
	}

	return false, false, ""
}
