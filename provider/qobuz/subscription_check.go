package qobuz

import (
	"encoding/json"
	"strings"
)

// isQobuzSubscriptionDegraded analyzes an error or response body from Qobuz API to determine
// if an account has lost its Hi-Res / Lossless streaming subscription (degraded to FREE),
// or if its token is permanently expired/invalid.
//
// Returns (degraded bool, isFatalAuth bool, reason string):
// - degraded: true if account lacks Hi-Res/Lossless streaming rights (demote tier to FREE)
// - isFatalAuth: true if token is completely invalid (deactivate account)
// - reason: descriptive reason for logging and database records
func isQobuzSubscriptionDegraded(err error, body string) (degraded bool, isFatalAuth bool, reason string) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	combined := strings.ToLower(errStr + " " + body)

	// 1. Fatal token errors
	if strings.Contains(combined, "invalid user_auth_token") ||
		strings.Contains(combined, "user_auth_token invalid") ||
		strings.Contains(combined, "invalid user token") ||
		strings.Contains(combined, "session expired") ||
		strings.Contains(combined, "invalid credentials") ||
		strings.Contains(combined, "user not found") ||
		strings.Contains(combined, "invalid_grant") ||
		strings.Contains(combined, "user/get: user not found") {
		return false, true, "user authentication token expired or invalid"
	}

	// 2. Subscription restrictions in getFileUrl
	if body != "" {
		var fileResp QobuzFileUrlResponse
		if jsonErr := json.Unmarshal([]byte(body), &fileResp); jsonErr == nil {
			for _, r := range fileResp.Restrictions {
				code := strings.ToLower(r.Code)
				if strings.Contains(code, "subscription") ||
					strings.Contains(code, "notallowed") ||
					strings.Contains(code, "userrestricted") {
					return true, false, "stream restriction: " + r.Code
				}
			}
		}
	}

	// 3. Explicit subscription degradation signals
	if strings.Contains(combined, "formatrestrictedbyusersubscription") ||
		strings.Contains(combined, "requires subscription") ||
		strings.Contains(combined, "preview only") ||
		strings.Contains(combined, "not entitled") ||
		strings.Contains(combined, "hires_streaming=false") ||
		strings.Contains(combined, "lossless_streaming=false") ||
		strings.Contains(combined, "subscription degraded") ||
		strings.Contains(combined, "subscription expired") {
		return true, false, "account lacks Hi-Res/Lossless streaming subscription"
	}

	return false, false, ""
}
