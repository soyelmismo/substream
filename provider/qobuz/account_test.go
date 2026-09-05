package qobuz

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"go.senan.xyz/gonic/db"
)

func TestQobuzSubscriptionDegraded(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		body         string
		wantDegraded bool
		wantFatal    bool
	}{
		{
			name:         "clean error",
			err:          errors.New("timeout"),
			wantDegraded: false,
			wantFatal:    false,
		},
		{
			name:         "invalid token",
			err:          errors.New("invalid user_auth_token"),
			wantDegraded: false,
			wantFatal:    true,
		},
		{
			name:         "requires subscription",
			err:          errors.New("qobuz track requires subscription (preview only)"),
			wantDegraded: true,
			wantFatal:    false,
		},
		{
			name:         "restriction by user subscription",
			err:          nil,
			body:         `{"restrictions":[{"code":"FormatRestrictedByUserSubscription"}]}`,
			wantDegraded: true,
			wantFatal:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			degraded, isFatal, _ := isQobuzSubscriptionDegraded(tt.err, tt.body)
			if degraded != tt.wantDegraded {
				t.Errorf("degraded=%v, want %v", degraded, tt.wantDegraded)
			}
			if isFatal != tt.wantFatal {
				t.Errorf("isFatal=%v, want %v", isFatal, tt.wantFatal)
			}
		})
	}
}

func TestQobuzAccountPoolRotation(t *testing.T) {
	pool := NewAccountPool(nil)

	accounts := []*db.QobuzAccount{
		{ID: 1, UserAuthToken: "tok1", Username: "User 1", QualityTier: 90, IsActive: true},
		{ID: 2, UserAuthToken: "tok2", Username: "User 2", QualityTier: 90, IsActive: true},
		{ID: 3, UserAuthToken: "tok3", Username: "User 3", QualityTier: 10, IsActive: true},
	}
	pool.SyncAccounts(accounts)

	if pool.Count() != 3 {
		t.Fatalf("expected 3 accounts, got %d", pool.Count())
	}

	// For Hi-Res (tier 85), only User 1 and User 2 should rotate
	seen := make(map[int]int)
	for i := 0; i < 10; i++ {
		sess, err := pool.GetNextAccount(85)
		if err != nil {
			t.Fatalf("GetNextAccount failed: %v", err)
		}
		seen[sess.Account.ID]++
	}

	if seen[3] > 0 {
		t.Errorf("Free account ID 3 returned for Hi-Res request")
	}
	if seen[1] != 5 || seen[2] != 5 {
		t.Errorf("expected even distribution between 1 and 2, got %v", seen)
	}

	// Verify HasLosslessAccount
	if !pool.HasLosslessAccount() {
		t.Errorf("expected pool to have lossless account")
	}
}

func TestQobuzAccountPoolDemoteToFree(t *testing.T) {
	tmpDB := "/tmp/test_qobuz_demote.db"
	_ = os.Remove(tmpDB)
	defer os.Remove(tmpDB)

	database, err := db.New(tmpDB)
	if err != nil {
		t.Fatalf("failed to create test DB: %v", err)
	}
	if err := database.Migrate(); err != nil {
		t.Fatalf("failed to migrate test DB: %v", err)
	}

	acc, err := database.AddQobuzAccount("test_tok_123", "user_1", "TestUser", "STUDIO", "24B192K", 90)
	if err != nil {
		t.Fatalf("failed to add account: %v", err)
	}

	pool := NewAccountPool(database)
	pool.SyncAccounts([]*db.QobuzAccount{acc})

	degradedInvoked := false
	var degradedID int
	pool.SetAccountDegradedHandler(func(id int, reason string) {
		degradedInvoked = true
		degradedID = id
	})

	// Trigger subscription degradation
	degradeErr := fmt.Errorf("qobuz track requires subscription (preview only)")
	pool.MarkError(acc.ID, degradeErr)

	if !degradedInvoked {
		t.Errorf("expected degraded handler to be invoked")
	}
	if degradedID != acc.ID {
		t.Errorf("expected degraded account ID %d, got %d", acc.ID, degradedID)
	}

	updatedAcc, err := database.GetQobuzAccountByID(acc.ID)
	if err != nil {
		t.Fatalf("failed to reload account: %v", err)
	}
	if updatedAcc.QualityTier != db.QualityTierFree {
		t.Errorf("expected QualityTier=%d, got %d", db.QualityTierFree, updatedAcc.QualityTier)
	}
	if updatedAcc.SubscriptionType != "FREE" {
		t.Errorf("expected SubscriptionType=FREE, got %s", updatedAcc.SubscriptionType)
	}
	if updatedAcc.IsActive {
		t.Errorf("expected IsActive=false after demotion to FREE")
	}
}
