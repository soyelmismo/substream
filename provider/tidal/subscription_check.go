package tidal

import (
	"encoding/json"
)

// isTidalSubscriptionDegraded returns true when a Tidal API error body
// indicates the account cannot stream at the requested quality — i.e. it
// is effectively FREE / unsubscribed, even if the DB row claims otherwise.
//
// Recognized signals (any one is enough):
//   - subStatus 5003: "Requested quality is not allowed in user's subscription"
//   - subStatus 4001: "User does not have a valid subscription"
//   - subStatus 4002: "User's subscription has expired"
//   - subStatus 4004: "User is not entitled for this resource"
//
// We do NOT treat "track not available in your region" / 404 / 403-track
// as a degraded-account signal, because those are track-specific, not
// account-specific, and demoting the account would be wrong.
func isTidalSubscriptionDegraded(body string) bool {
	if body == "" {
		return false
	}
	var probe struct {
		SubStatus   int    `json:"subStatus"`
		UserMessage string `json:"userMessage"`
	}
	if err := json.Unmarshal([]byte(body), &probe); err != nil {
		return false
	}
	// Tidal documented codes that mean "this account cannot stream at the requested quality":
	//   5003 — "Requested quality is not allowed in user's subscription"
	//   4001 — "User does not have a valid subscription"
	//   4002 — "User's subscription has expired"
	//   4004 — "User is not entitled for this resource"
	switch probe.SubStatus {
	case 5003, 4001, 4002, 4004:
		return true
	}
	return false
}

// truncate returns at most n bytes of s, appending an ellipsis if it
// was shortened. Used so log lines for subscription errors stay bounded.
func truncate(s string, n int) string {
	if n < 0 {
		n = 0
	}
	if len(s) <= n {
		return s
	}
	if n < 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
