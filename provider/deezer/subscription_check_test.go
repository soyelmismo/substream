package deezer

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"go.senan.xyz/gonic/db"
)

func TestDeezerSubscriptionDegraded(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		body         string
		wantDegraded bool
		wantFatal    bool
	}{
		{
			name:         "clean error",
			err:          errors.New("connection reset by peer"),
			wantDegraded: false,
			wantFatal:    false,
		},
		{
			name:         "invalid ARL",
			err:          errors.New("invalid ARL: session expired"),
			wantDegraded: false,
			wantFatal:    true,
		},
		{
			name:         "WebLossless false signal",
			err:          errors.New("account degraded: WebLossless=false for user test"),
			wantDegraded: true,
			wantFatal:    false,
		},
		{
			name:         "subscription degraded error",
			err:          errors.New("subscription degraded: not entitled for FLAC"),
			wantDegraded: true,
			wantFatal:    false,
		},
		{
			name:         "media error code 2000 unauthorized",
			err:          nil,
			body:         `{"errors":[{"code":2000,"message":"unauthorized"}]}`,
			wantDegraded: true,
			wantFatal:    false,
		},
		{
			name:         "track media error code 2000 format not allowed",
			err:          nil,
			body:         `{"data":[{"errors":[{"code":2000,"message":"format not allowed"}]}]}`,
			wantDegraded: true,
			wantFatal:    false,
		},
		{
			name:         "media error code 1000 license expired",
			err:          nil,
			body:         `{"errors":[{"code":1000,"message":"license token expired"}]}`,
			wantDegraded: false,
			wantFatal:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			degraded, isFatal, reason := isDeezerSubscriptionDegraded(tt.err, tt.body)
			if degraded != tt.wantDegraded {
				t.Errorf("degraded = %v, want %v (reason: %s)", degraded, tt.wantDegraded, reason)
			}
			if isFatal != tt.wantFatal {
				t.Errorf("isFatal = %v, want %v (reason: %s)", isFatal, tt.wantFatal, reason)
			}
		})
	}
}

func TestAccountPoolDemoteToFree(t *testing.T) {
	tmpDB := "/tmp/test_deezer_demote.db"
	_ = os.Remove(tmpDB)
	defer os.Remove(tmpDB)

	database, err := db.New(tmpDB)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	if err := database.Migrate(); err != nil {
		t.Fatalf("failed to migrate test DB: %v", err)
	}

	acc, err := database.AddDeezerAccount("dummy_arl_123", "user_1", "TestUser", "PREMIUM", "FLAC", db.QualityTierLossless)
	if err != nil {
		t.Fatalf("failed to add test account: %v", err)
	}

	pool := NewAccountPool(database)
	pool.SyncAccounts([]*db.DeezerAccount{acc})

	degradedInvoked := false
	var degradedID int
	var degradedReason string
	pool.SetAccountDegradedHandler(func(id int, reason string) {
		degradedInvoked = true
		degradedID = id
		degradedReason = reason
	})

	// Trigger subscription degradation
	degradeErr := fmt.Errorf("account degraded: WebLossless=false for user %s", acc.Username)
	pool.MarkError(acc.ID, degradeErr)

	if !degradedInvoked {
		t.Errorf("expected degraded handler to be invoked")
	}
	if degradedID != acc.ID {
		t.Errorf("expected degraded account ID %d, got %d", acc.ID, degradedID)
	}
	if degradedReason == "" {
		t.Errorf("expected non-empty degradation reason")
	}

	// Verify DB record is demoted to FREE and tier 10
	updatedAcc, err := database.GetDeezerAccountByID(acc.ID)
	if err != nil {
		t.Fatalf("failed to reload account: %v", err)
	}
	if updatedAcc.QualityTier != db.QualityTierFree {
		t.Errorf("expected QualityTier=%d (Free), got %d", db.QualityTierFree, updatedAcc.QualityTier)
	}
	if updatedAcc.SubscriptionType != "FREE" {
		t.Errorf("expected SubscriptionType=FREE, got %s", updatedAcc.SubscriptionType)
	}
	if updatedAcc.IsActive {
		t.Errorf("expected IsActive=false after demote to FREE")
	}
}
