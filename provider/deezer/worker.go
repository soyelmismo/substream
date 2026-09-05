package deezer

import (
	"context"
	"log"
	"time"

	"go.senan.xyz/gonic/db"
)

// BackgroundWorker periodically validates active Deezer accounts, refreshing tokens
// and demoting accounts that have lost Lossless rights or deactivating expired accounts.
type BackgroundWorker struct {
	dbc      *db.DB
	pool     *AccountPool
	interval time.Duration
	stopCh   chan struct{}
}

// NewBackgroundWorker creates a worker with the given check interval.
func NewBackgroundWorker(dbc *db.DB, pool *AccountPool, interval time.Duration) *BackgroundWorker {
	if interval <= 0 {
		interval = 1 * time.Hour
	}
	return &BackgroundWorker{
		dbc:      dbc,
		pool:     pool,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start runs the periodic validation loop in the background.
func (w *BackgroundWorker) Start() {
	if w == nil || w.dbc == nil || w.pool == nil {
		return
	}
	go func() {
		// Stagger initial check slightly to let server start up
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
	log.Printf("[DEEZER] 🔄 Background account maintenance worker started (interval: %v)", w.interval)
}

// Stop terminates the background worker.
func (w *BackgroundWorker) Stop() {
	if w != nil && w.stopCh != nil {
		close(w.stopCh)
	}
}

func (w *BackgroundWorker) runMaintenance() {
	accounts, err := w.dbc.GetDeezerAccounts()
	if err != nil || len(accounts) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, acc := range accounts {
		if !acc.IsActive {
			continue
		}

		info, vErr := ValidateARL(ctx, acc.ARL)
		if vErr != nil {
			degraded, isFatal, reason := isDeezerSubscriptionDegraded(vErr, "")
			if isFatal {
				log.Printf("[DEEZER] ⚠️ Background check: account %s (id=%d) ARL invalid/expired (%v). Deactivating.", acc.Username, acc.ID, vErr)
				_ = w.dbc.ToggleDeezerAccount(acc.ID, false)
				_ = w.dbc.UpdateDeezerAccountError(acc.ID, vErr.Error())
			} else if degraded {
				log.Printf("[DEEZER] ⚠️ Background check: account %s (id=%d) subscription degraded (%s). Demoting to FREE.", acc.Username, acc.ID, reason)
				_ = w.dbc.MarkDeezerAccountAsFree(acc.ID)
			} else {
				_ = w.dbc.UpdateDeezerAccountError(acc.ID, vErr.Error())
			}
			continue
		}

		// Check if subscription was downgraded from Lossless
		if acc.QualityTier >= db.QualityTierLossless && !info.WebLossless {
			log.Printf("[DEEZER] ⛔ Background check: account %s (id=%d) has lost Lossless entitlement (WebLossless=false). Demoting to FREE.", acc.Username, acc.ID)
			_ = w.dbc.MarkDeezerAccountAsFree(acc.ID)
			continue
		}

		// Account is healthy: update metadata
		_ = w.dbc.UpdateDeezerAccountSubscription(acc.ID, info.SubscriptionType, info.HighestQuality, info.QualityTier, "")
	}

	// Resync active accounts to pool
	if freshAccs, err := w.dbc.GetActiveDeezerAccounts(); err == nil {
		w.pool.SyncAccounts(freshAccs)
	}
}
