package tidalproxy

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MirrorState represents the health state of a mirror
type MirrorState int

const (
	StateHealthy MirrorState = iota
	StateUnhealthy
	StateProbing
)

func (s MirrorState) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateUnhealthy:
		return "unhealthy"
	case StateProbing:
		return "probing"
	default:
		return "unknown"
	}
}

// Mirror represents a single proxy mirror
type Mirror struct {
	URL          string
	Weight       int
	HealthEndpoint string // e.g., "/health" or just "/" for HEAD check
	
	// Runtime stats - accessed via atomic
	state          atomic.Int32 // MirrorState
	latencyEMA     atomic.Int64 // nanoseconds, exponential moving average
	failCount      atomic.Int32
	activeRequests atomic.Int32
	lastFail       atomic.Int64 // unix timestamp
	lastSuccess    atomic.Int64 // unix timestamp
	requestCount   atomic.Int64
}

// MirrorManager manages multiple proxy mirrors with health checking
type MirrorManager struct {
	mirrors []*Mirror
	mu      sync.RWMutex
	
	// Config
	healthCheckInterval time.Duration
	failureThreshold    int
	cooldownDuration    time.Duration
	probeTimeout        time.Duration
	
	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	gate   chan struct{}
}

// MirrorConfig for initializing mirrors
type MirrorConfig struct {
	URL            string
	Weight         int
	HealthEndpoint string
}

// NewMirrorManager creates a new mirror manager
func NewMirrorManager(configs []MirrorConfig, healthCheckInterval time.Duration) *MirrorManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	mm := &MirrorManager{
		mirrors:             make([]*Mirror, 0, len(configs)),
		healthCheckInterval: healthCheckInterval,
		failureThreshold:    3,
		cooldownDuration:    30 * time.Second,
		probeTimeout:        5 * time.Second,
		ctx:                 ctx,
		cancel:              cancel,
	}
	
	for _, cfg := range configs {
		m := &Mirror{
			URL:            cfg.URL,
			Weight:         cfg.Weight,
			HealthEndpoint: cfg.HealthEndpoint,
		}
		m.state.Store(int32(StateHealthy))
		m.latencyEMA.Store(int64(100 * time.Millisecond)) // Initial estimate
		mm.mirrors = append(mm.mirrors, m)
	}

	mm.gate = make(chan struct{}, len(mm.mirrors))
	
	return mm
}

// Start begins background health checking
func (mm *MirrorManager) Start() {
	mm.wg.Add(1)
	go mm.healthCheckLoop()
	log.Printf("[MIRROR] Manager started with %d mirrors", len(mm.mirrors))
}

// UpdateMirrors updates the mirror list with new URLs (preserves stats for existing mirrors)
func (mm *MirrorManager) UpdateMirrors(urls []string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Build map of existing mirrors for quick lookup
	existing := make(map[string]*Mirror)
	for _, m := range mm.mirrors {
		existing[m.URL] = m
	}

	// Build new mirror list
	newMirrors := make([]*Mirror, 0, len(urls))
	for _, url := range urls {
		url = strings.TrimSuffix(url, "/")
		
		if m, ok := existing[url]; ok {
			// Keep existing mirror with its stats
			newMirrors = append(newMirrors, m)
		} else {
			// Create new mirror
			m := &Mirror{
				URL:            url,
				Weight:         100, // Default weight
				HealthEndpoint: "/info/?id=1",
			}
			m.state.Store(int32(StateHealthy))
			m.latencyEMA.Store(int64(100 * time.Millisecond))
			newMirrors = append(newMirrors, m)
			log.Printf("[MIRROR] Added new mirror: %s", url)
		}
	}

	// Log removed mirrors
	for url, m := range existing {
		found := false
		for _, newURL := range urls {
			if strings.TrimSuffix(newURL, "/") == url {
				found = true
				break
			}
		}
		if !found {
			log.Printf("[MIRROR] Removed mirror: %s (state=%s, reqs=%d)", 
				url, m.GetState(), m.requestCount.Load())
		}
	}

	mm.mirrors = newMirrors
	log.Printf("[MIRROR] Updated mirror list: %d mirrors active", len(mm.mirrors))
}

// Stop shuts down the manager
func (mm *MirrorManager) Stop() {
	mm.cancel()
	mm.wg.Wait()
}

// SelectMirror chooses the best mirror based on health and latency
// AcquireToken waits for a proxy to become available
func (mm *MirrorManager) AcquireToken(ctx context.Context) error {
	select {
	case mm.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseToken frees a proxy for the next request in queue
func (mm *MirrorManager) ReleaseToken() {
	<-mm.gate
}

func (mm *MirrorManager) SelectMirror() *Mirror {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var healthyMirrors []*Mirror
	for _, m := range mm.mirrors {
		if m.GetState() == StateHealthy {
			healthyMirrors = append(healthyMirrors, m)
		}
	}

	if len(healthyMirrors) == 0 {
		return nil
	}

	// [New Strategy] Preference for IDLE mirrors first (activeRequests == 0)
	var idleMirrors []*Mirror
	for _, m := range healthyMirrors {
		if m.activeRequests.Load() == 0 {
			idleMirrors = append(idleMirrors, m)
		}
	}

	// Use idle pool if available, otherwise use all healthy ones
	selectionPool := idleMirrors
	if len(selectionPool) == 0 {
		selectionPool = healthyMirrors
	}

	if len(selectionPool) == 1 {
		return selectionPool[0]
	}

	// Weighted Random Selection (Shotgun Spread)
	var totalScore float64
	type scoredMirror struct {
		m     *Mirror
		limit float64
	}
	var scoredPool []scoredMirror

	for _, m := range selectionPool {
		_, latency, _, _ := m.GetStats()
		// Score = (1/latency) * weight
		score := 1.0 / (float64(latency+1) / float64(time.Millisecond)) * float64(m.Weight)
		totalScore += score
		scoredPool = append(scoredPool, scoredMirror{m: m, limit: totalScore})
	}

	r := rand.Float64() * totalScore
	for _, sm := range scoredPool {
		if r <= sm.limit {
			return sm.m
		}
	}

	return selectionPool[0]
}

// ReportResult updates mirror stats after a request
func (mm *MirrorManager) ReportResult(m *Mirror, latency time.Duration, err error) {
	m.requestCount.Add(1)
	
	if err != nil {
		m.failCount.Add(1)
		m.lastFail.Store(time.Now().Unix())
		
		errStr := err.Error()
		isRateLimit := strings.Contains(errStr, "429")

		// Check if we should circuit break
		// [New] 429 (Rate Limit) causes immediate unhealthy state
		if (isRateLimit || int(m.failCount.Load()) >= mm.failureThreshold) && m.GetState() == StateHealthy {
			m.SetState(StateUnhealthy)
			reason := "multiple failures"
			if isRateLimit { reason = "rate limited (429)" }
			log.Printf("[MIRROR] %s marked unhealthy: %s", m.URL, reason)
		}
		return
	}
	
	// Success - update latency EMA
	m.lastSuccess.Store(time.Now().Unix())
	m.failCount.Store(0)
	
	oldEMA := m.latencyEMA.Load()
	newSample := int64(latency)
	
	// EMA: 0.7 * old + 0.3 * new
	newEMA := int64(0.7*float64(oldEMA) + 0.3*float64(newSample))
	m.latencyEMA.Store(newEMA)
	
	// If was probing and succeeded, mark healthy
	if m.GetState() == StateProbing {
		m.SetState(StateHealthy)
		log.Printf("[MIRROR] %s recovered and marked healthy", m.URL)
	}
}

// GetState returns current state
func (m *Mirror) GetState() MirrorState {
	return MirrorState(m.state.Load())
}

// SetState updates state
func (m *Mirror) SetState(s MirrorState) {
	m.state.Store(int32(s))
}

// GetStats returns current stats for logging/monitoring
func (m *Mirror) GetStats() (state MirrorState, latency time.Duration, failCount int, reqs int64) {
	return m.GetState(), 
		time.Duration(m.latencyEMA.Load()), 
		int(m.failCount.Load()), 
		m.requestCount.Load()
}

// healthCheckLoop periodically checks mirror health
func (mm *MirrorManager) healthCheckLoop() {
	defer mm.wg.Done()
	
	ticker := time.NewTicker(mm.healthCheckInterval)
	defer ticker.Stop()
	
	// Initial check
	mm.runHealthChecks()
	
	for {
		select {
		case <-mm.ctx.Done():
			return
		case <-ticker.C:
			mm.runHealthChecks()
		}
	}
}

// runHealthChecks checks all mirrors
func (mm *MirrorManager) runHealthChecks() {
	client := &http.Client{
		Timeout: mm.probeTimeout,
	}
	
	for _, m := range mm.mirrors {
		state := m.GetState()
		
		// Only check unhealthy (for cooldown) or all periodically
		if state == StateHealthy && rand.Float32() > 0.2 {
			// Skip 80% of healthy checks to reduce load
			continue
		}
		
		go mm.checkMirror(client, m)
	}
}

// checkMirror probes a single mirror
func (mm *MirrorManager) checkMirror(client *http.Client, m *Mirror) {
	m.activeRequests.Add(1)
	defer m.activeRequests.Add(-1)

	start := time.Now()
	
	endpoint := m.URL + m.HealthEndpoint
	if m.HealthEndpoint == "" {
		endpoint = m.URL // Use base URL
	}
	
	ctx, cancel := context.WithTimeout(mm.ctx, mm.probeTimeout)
	defer cancel()
	
	req, err := http.NewRequestWithContext(ctx, "HEAD", endpoint, nil)
	if err != nil {
		return
	}
	
	resp, err := client.Do(req)
	latency := time.Since(start)
	
	// Final health check - if it was marked as unhealthy while we were starting, fail fast
	if m.GetState() == StateUnhealthy {
		if resp != nil { resp.Body.Close() }
		return
	}

	if err != nil {
		// Failed - might be network error
		if m.GetState() == StateHealthy {
			m.failCount.Add(1)
			if int(m.failCount.Load()) >= mm.failureThreshold {
				m.SetState(StateUnhealthy)
				log.Printf("[MIRROR] %s health check failed, marked unhealthy", m.URL)
			}
		}
		return
	}
	resp.Body.Close()
	
	// Success
	if m.GetState() == StateUnhealthy {
		// Check if cooldown passed
		lastFail := time.Unix(m.lastFail.Load(), 0)
		if time.Since(lastFail) > mm.cooldownDuration {
			m.SetState(StateProbing)
			log.Printf("[MIRROR] %s cooldown ended, entering probing state", m.URL)
		}
	} else if m.GetState() == StateProbing {
		m.SetState(StateHealthy)
		m.failCount.Store(0)
		log.Printf("[MIRROR] %s health check passed, marked healthy (latency: %v)", m.URL, latency)
	}
	
	// Update latency even on health checks
	mm.ReportResult(m, latency, nil)
}

// GetStatus returns full status for all mirrors (for debugging)
func (mm *MirrorManager) GetStatus() string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	
	status := "Mirror Status:\n"
	for _, m := range mm.mirrors {
		state, latency, fails, reqs := m.GetStats()
		status += fmt.Sprintf("  %s: %s | latency=%v | fails=%d | reqs=%d | weight=%d\n",
			m.URL, state, latency, fails, reqs, m.Weight)
	}
	return status
}

// GetMirrors returns the list of mirrors (readonly)
func (mm *MirrorManager) GetMirrors() []*Mirror {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.mirrors
}
