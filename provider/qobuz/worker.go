package qobuz

import (
	"context"
	"log"
	"time"

	"go.senan.xyz/gonic/db"
)

// BackgroundWorker periodically validates active Qobuz accounts, refreshing subscription
// state and demoting accounts that have lost Hi-Res/Lossless rights or deactivating expired tokens.
type BackgroundWorker struct {
	dbc       *db.DB
	pool      *AccountPool
	appID     string
	appSecret string
	interval  time.Duration
	stopCh    chan struct{}
}

// NewBackgroundWorker creates a worker with the given check interval
func NewBackgroundWorker(dbc *db.DB, pool *AccountPool, appID, appSecret string, interval time.Duration) *BackgroundWorker {
	if interval <= 0 {
		interval = 1 * time.Hour
	}
	return &BackgroundWorker{
		dbc:       dbc,
		pool:      pool,
		appID:     appID,
		appSecret: appSecret,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
}

// Start runs the periodic validation loop in the background
func (w *BackgroundWorker) Start() {
	if w == nil || w.dbc == nil || w.pool == nil {
		return
	}
	go func() {
		time.Sleep(10 * time.Second)
		w.runMaintenance()

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.runMaintenance()
			case <-w.stopCh:
				return
			}
		}
	}()
	log.Printf("[QOBUZ] 🔄 Background account maintenance worker started (interval: %v)", w.interval)
}

// Stop terminates the background worker
func (w *BackgroundWorker) Stop() {
	if w != nil && w.stopCh != nil {
		close(w.stopCh)
	}
}

func (w *BackgroundWorker) runMaintenance() {
	accounts, err := w.dbc.GetQobuzAccounts()
	if err != nil || len(accounts) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, acc := range accounts {
		if !acc.IsActive {
			continue
		}

		info, vErr := ValidateToken(ctx, w.appID, w.appSecret, acc.UserAuthToken)
		if vErr != nil {
			degraded, isFatal, reason := isQobuzSubscriptionDegraded(vErr, "")
			if isFatal {
				log.Printf("[QOBUZ] ⚠️ Background check: account %s (id=%d) token invalid/expired (%v). Deactivating.", acc.Username, acc.ID, vErr)
				_ = w.dbc.ToggleQobuzAccount(acc.ID, false)
				_ = w.dbc.UpdateQobuzAccountError(acc.ID, vErr.Error())
			} else if degraded {
				log.Printf("[QOBUZ] ⚠️ Background check: account %s (id=%d) subscription degraded (%s). Demoting to FREE.", acc.Username, acc.ID, reason)
				_ = w.dbc.MarkQobuzAccountAsFree(acc.ID)
			} else {
				_ = w.dbc.UpdateQobuzAccountError(acc.ID, vErr.Error())
			}
			continue
		}

		// Check if subscription was downgraded from Hi-Res / Lossless
		if acc.QualityTier >= db.QualityTierLossless && !info.LosslessStreaming && !info.HiresStreaming {
			log.Printf("[QOBUZ] ⛔ Background check: account %s (id=%d) has lost streaming rights. Demoting to FREE.", acc.Username, acc.ID)
			_ = w.dbc.MarkQobuzAccountAsFree(acc.ID)
			continue
		}

		// Account is healthy: update metadata
		_ = w.dbc.UpdateQobuzAccountSubscription(acc.ID, info.SubscriptionType, info.HighestQuality, info.QualityTier, "")
	}

	// Resync active accounts to pool
	if freshAccs, err := w.dbc.GetActiveQobuzAccounts(); err == nil {
		w.pool.SyncAccounts(freshAccs)
	}
}
