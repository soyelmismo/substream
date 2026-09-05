package qobuz

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.senan.xyz/gonic/db"
)

// ValidateToken validates a user_auth_token against the Qobuz API and returns account info
func ValidateToken(ctx context.Context, appID, appSecret, token string) (*QobuzAccountInfo, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("empty user_auth_token")
	}

	client := NewClient(ClientOptions{
		AppID:         appID,
		AppSecret:     appSecret,
		UserAuthToken: token,
	})

	var res QobuzUserGetResponse
	if err := client.doSignedRequest(ctx, "user/get", nil, &res); err != nil {
		return nil, fmt.Errorf("user/get request failed: %w", err)
	}

	var userID string
	switch v := res.ID.(type) {
	case float64:
		userID = fmt.Sprintf("%.0f", v)
	case string:
		userID = v
	case int:
		userID = fmt.Sprintf("%d", v)
	case int64:
		userID = fmt.Sprintf("%d", v)
	default:
		userID = fmt.Sprintf("%v", v)
	}

	if userID == "" || userID == "0" {
		return nil, fmt.Errorf("qobuz session invalid or user not found")
	}

	username := res.DisplayName
	if username == "" {
		username = res.Login
	}
	if username == "" {
		username = res.Email
	}
	if username == "" {
		username = fmt.Sprintf("Qobuz User %s", userID)
	}

	subType := "FREE"
	highestQuality := "MP3_320"
	qualityTier := db.QualityTierFree

	params := res.Credential.Parameters
	if params.HiresStreaming {
		subType = "STUDIO"
		highestQuality = "24B192K"
		qualityTier = 90
	} else if params.LosslessStreaming {
		subType = "PREMIUM"
		highestQuality = "16B44K"
		qualityTier = db.QualityTierLossless
	}

	return &QobuzAccountInfo{
		UserID:            userID,
		Username:          username,
		SubscriptionType:  subType,
		HighestQuality:    highestQuality,
		QualityTier:       qualityTier,
		HiresStreaming:    params.HiresStreaming,
		LosslessStreaming: params.LosslessStreaming,
		RawResponse:       &res,
	}, nil
}

// AccountSession manages a single authenticated Qobuz account
type AccountSession struct {
	Account *db.QobuzAccount
	mu      sync.Mutex
}

// NewAccountSession creates a session for an account
func NewAccountSession(acc *db.QobuzAccount) *AccountSession {
	return &AccountSession{
		Account: acc,
	}
}

// AccountPool maintains active Qobuz accounts and performs round-robin rotation.
type AccountPool struct {
	mu                sync.RWMutex
	sessions          []*AccountSession
	rrIndex           uint64
	db                *db.DB
	onAccountDegraded func(accountID int, reason string)
}

// NewAccountPool creates an empty AccountPool
func NewAccountPool(dbc *db.DB) *AccountPool {
	return &AccountPool{
		db: dbc,
	}
}

// SetDB sets or updates the database reference
func (p *AccountPool) SetDB(dbc *db.DB) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.db = dbc
}

// SetAccountDegradedHandler sets a callback invoked when an account is demoted due to degraded subscription
func (p *AccountPool) SetAccountDegradedHandler(fn func(accountID int, reason string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onAccountDegraded = fn
}

// SyncAccounts synchronizes active accounts with the database
func (p *AccountPool) SyncAccounts(accounts []*db.QobuzAccount) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var updated []*AccountSession
	for _, acc := range accounts {
		if !acc.IsActive {
			continue
		}
		updated = append(updated, NewAccountSession(acc))
	}

	p.sessions = updated
	log.Printf("[QOBUZ] Account pool synchronized with %d active account(s)", len(updated))
}

// GetNextAccount selects the next available account meeting minTier using round-robin
func (p *AccountPool) GetNextAccount(minTier int) (*AccountSession, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.sessions)
	if n == 0 {
		return nil, fmt.Errorf("no active Qobuz accounts available in pool")
	}

	var eligible []*AccountSession
	for _, s := range p.sessions {
		if s.Account != nil && s.Account.IsActive && s.Account.QualityTier >= minTier {
			eligible = append(eligible, s)
		}
	}

	if len(eligible) == 0 {
		for _, s := range p.sessions {
			if s.Account != nil && s.Account.IsActive {
				eligible = append(eligible, s)
			}
		}
	}

	if len(eligible) == 0 {
		return nil, fmt.Errorf("no active Qobuz accounts configured")
	}

	idx := atomic.AddUint64(&p.rrIndex, 1) % uint64(len(eligible))
	return eligible[idx], nil
}

// MarkError records an error on an account and persists it to the database if available
func (p *AccountPool) MarkError(accountID int, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
		return
	}
	errStr := err.Error()
	degraded, isFatalAuth, reason := isQobuzSubscriptionDegraded(err, "")

	var degradedHandler func(int, string)
	deactivated := isFatalAuth
	demoted := degraded

	p.mu.Lock()
	degradedHandler = p.onAccountDegraded
	for _, s := range p.sessions {
		if s.Account != nil && s.Account.ID == accountID {
			s.Account.LastError = errStr
			s.Account.LastValidatedAt = time.Now()
			if isFatalAuth {
				s.Account.IsActive = false
			} else if degraded {
				s.Account.QualityTier = db.QualityTierFree
				s.Account.SubscriptionType = "FREE"
				s.Account.HighestQuality = "MP3_320"
				s.Account.IsActive = false
			}
			break
		}
	}
	p.mu.Unlock()

	if p.db != nil {
		_ = p.db.UpdateQobuzAccountError(accountID, errStr)
		if demoted {
			log.Printf("[QOBUZ] ⛔ Demoting account id=%d to FREE — reason: %s", accountID, reason)
			_ = p.db.MarkQobuzAccountAsFree(accountID)
		} else if deactivated {
			log.Printf("[QOBUZ] ⛔ Deactivating account id=%d — reason: %s", accountID, reason)
			_ = p.db.ToggleQobuzAccount(accountID, false)
		}
	}

	if demoted && degradedHandler != nil {
		degradedHandler(accountID, reason)
	}
}

// Count returns total active sessions in the pool
func (p *AccountPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := 0
	for _, s := range p.sessions {
		if s.Account != nil && s.Account.IsActive {
			count++
		}
	}
	return count
}

// HasLosslessAccount returns true if any active account supports FLAC/Hi-Res
func (p *AccountPool) HasLosslessAccount() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, s := range p.sessions {
		if s.Account != nil && s.Account.IsActive && s.Account.QualityTier >= db.QualityTierLossless {
			return true
		}
	}
	return false
}
