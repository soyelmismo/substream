package tidalproxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
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
	URL            string
	Weight         int
	HealthEndpoint string // e.g., "/health" or just "/" for HEAD check

	// Runtime stats - accessed via atomic
	state               atomic.Int32 // MirrorState
	latencyEMA          atomic.Int64 // nanoseconds, exponential moving average
	failCount           atomic.Int32
	successCount        atomic.Int64 // for success rate calculation
	consecutiveSuccess  atomic.Int32 // for hysteresis in recovery
	activeRequests      atomic.Int32
	lastFail            atomic.Int64 // unix timestamp of last request failure
	lastSuccess         atomic.Int64 // unix timestamp
	requestCount        atomic.Int64
	lastHealthCheckFail atomic.Int64 // unix timestamp of last health check failure

	// --- NUEVAS MÉTRICAS DE STREAMING ---
	btsSuccesses atomic.Int64
	hlsResponses atomic.Int64
	streamFails  atomic.Int64
}

// Nuevos métodos de reporte
func (m *Mirror) ReportBTS()        { m.btsSuccesses.Add(1) }
func (m *Mirror) ReportHLS()        { m.hlsResponses.Add(1) }
func (m *Mirror) ReportStreamFail() { m.streamFails.Add(1) }

// GetStreamScore calcula la confiabilidad del mirror basándose en los codecs que entrega
func (m *Mirror) GetStreamScore() float64 {
	bts := float64(m.btsSuccesses.Load())
	hls := float64(m.hlsResponses.Load())
	fails := float64(m.streamFails.Load())
	lat := float64(m.latencyEMA.Load()) / float64(time.Millisecond)
	if lat <= 0 {
		lat = 1
	}

	// Base inicial por latencia (100ms = 100pts, 50ms = 200pts)
	score := 10000.0 / lat

	// Sistema de Karma (Historial de comportamiento)
	score += (bts * 50.0)    // Fuerte recompensa por dar BTS Progresivo
	score -= (hls * 10.0)    // Ligera penalización por dar HLS
	score -= (fails * 150.0) // CASTIGO SEVERO por quedarse colgado o fallar

	return score
}

// GetAPIScore calcula la confiabilidad general para metadata
func (m *Mirror) GetAPIScore() float64 {
	lat := float64(m.latencyEMA.Load()) / float64(time.Millisecond)
	if lat <= 0 {
		lat = 1
	}
	// Latencia combinada con Tasa de Éxito
	return (10000.0 / lat) * m.GetSuccessRate()
}

// GetMirrorsByPriority devuelve el pool completo ordenado inteligentemente según la tarea
func (mm *MirrorManager) GetMirrorsByPriority(priority TaskPriority) []*Mirror {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var pool []*Mirror
	for _, m := range mm.mirrors {
		if m.GetState() == StateHealthy || m.GetState() == StateProbing {
			pool = append(pool, m)
		}
	}

	if len(pool) == 0 {
		return nil
	}

	switch priority {
	case PriorityUrgent:
		// Streaming: Los reyes del Stream Score van primero
		sort.Slice(pool, func(i, j int) bool {
			return pool[i].GetStreamScore() > pool[j].GetStreamScore()
		})
	case PriorityNormal:
		// Metadata: Ordenar por API Score
		sort.Slice(pool, func(i, j int) bool {
			return pool[i].GetAPIScore() > pool[j].GetAPIScore()
		})
		// Soft-Knee: Si hay suficientes proxies, empujar al campeón (#0 y #1) más atrás
		// para no saturarlo con peticiones JSON tontas.
		if len(pool) >= 4 {
			p0, p1 := pool[0], pool[1]
			pool[0] = pool[2]
			pool[1] = pool[3]
			pool[2] = p0
			pool[3] = p1
		}
	case PriorityBackground:
		// Hidratación de fondo: Ordenar por API Score pero INVERTIR EL ARREGLO
		// (Los más lentos/apestados pero sanos se usan primero)
		sort.Slice(pool, func(i, j int) bool {
			return pool[i].GetAPIScore() < pool[j].GetAPIScore() // Menor a mayor
		})
	}

	return pool
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

	// Benchmark debounce - prevent consecutive benchmark runs
	lastBenchmarkTime atomic.Int64 // unix timestamp in nanoseconds
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

	return mm
}

// Start begins background health checking
func (mm *MirrorManager) Start() {
	mm.wg.Add(1)
	go mm.healthCheckLoop()
	log.Printf("[MIRROR] Manager started with %d mirrors (latency benchmark runs after mirror discovery)", len(mm.mirrors))
}

// BenchmarkMirrors measures latency of all mirrors for initial sorting
func (mm *MirrorManager) BenchmarkMirrors() {
	mm.mu.RLock()
	mirrors := make([]*Mirror, len(mm.mirrors))
	copy(mirrors, mm.mirrors)
	mm.mu.RUnlock()

	if len(mirrors) == 0 {
		return
	}

	log.Printf("[MIRROR] Benchmarking %d mirrors for latency sorting...", len(mirrors))

	var wg sync.WaitGroup
	results := make(chan struct {
		mirror  *Mirror
		latency time.Duration
	}, len(mirrors))

	for _, m := range mirrors {
		wg.Add(1)
		go func(mirror *Mirror) {
			defer wg.Done()

			// Quick ping to measure latency (2 attempts, take fastest)
			var bestLatency time.Duration = time.Second

			for i := 0; i < 2; i++ {
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				endpoint := mirror.URL + mirror.HealthEndpoint
				req, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)

				client := &http.Client{Timeout: 3 * time.Second}
				resp, err := client.Do(req)
				cancel()

				if err == nil && resp != nil {
					resp.Body.Close()
					latency := time.Since(start)
					if latency < bestLatency {
						bestLatency = latency
					}
				}
			}

			results <- struct {
				mirror  *Mirror
				latency time.Duration
			}{mirror, bestLatency}
		}(m)
	}

	// Close results channel when all done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var sortedMirrors []*Mirror
	for r := range results {
		if r.latency < time.Second {
			r.mirror.latencyEMA.Store(int64(r.latency))
			sortedMirrors = append(sortedMirrors, r.mirror)
			log.Printf("[MIRROR] %s latency: %v", r.mirror.URL, r.latency)
		} else {
			log.Printf("[MIRROR] %s latency: timeout/unreachable", r.mirror.URL)
		}
	}

	// Sort mirrors by latency
	mm.mu.Lock()
	for i := 0; i < len(mm.mirrors)-1; i++ {
		for j := i + 1; j < len(mm.mirrors); j++ {
			if mm.mirrors[i].latencyEMA.Load() > mm.mirrors[j].latencyEMA.Load() {
				mm.mirrors[i], mm.mirrors[j] = mm.mirrors[j], mm.mirrors[i]
			}
		}
	}
	mm.mu.Unlock()

	log.Printf("[MIRROR] Benchmark complete. Fastest: %s (%v)",
		mm.mirrors[0].URL, time.Duration(mm.mirrors[0].latencyEMA.Load()))
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
				HealthEndpoint: "/", // Use root endpoint - just check if server responds
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

	// Run benchmark if we have enough mirrors and debounce period has passed
	if len(newMirrors) >= 5 {
		lastBench := mm.lastBenchmarkTime.Load()
		now := time.Now().UnixNano()
		// Debounce: only run if 5 seconds have passed since last benchmark
		if now-lastBench > int64(5*time.Second) {
			if mm.lastBenchmarkTime.CompareAndSwap(lastBench, now) {
				// Run in background to not block the update
				go mm.BenchmarkMirrors()
			}
		}
	}
}

// Stop shuts down the manager
func (mm *MirrorManager) Stop() {
	mm.cancel()
	mm.wg.Wait()
}

// SelectMirror chooses the best mirror based on health and latency
func (mm *MirrorManager) SelectMirror() *Mirror {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var healthyMirrors []*Mirror
	var probingMirrors []*Mirror
	for _, m := range mm.mirrors {
		switch m.GetState() {
		case StateHealthy:
			healthyMirrors = append(healthyMirrors, m)
		case StateProbing:
			probingMirrors = append(probingMirrors, m)
		}
	}

	// Priority: healthy > probing > nil
	selectionPool := healthyMirrors
	if len(selectionPool) == 0 {
		selectionPool = probingMirrors
		if len(selectionPool) > 0 {
			log.Printf("[MIRROR] No healthy mirrors, falling back to %d probing mirrors", len(selectionPool))
		}
	}

	if len(selectionPool) == 0 {
		return nil
	}

	// [New Strategy] Preference for IDLE mirrors first (activeRequests == 0)
	var idleMirrors []*Mirror
	for _, m := range selectionPool {
		if m.activeRequests.Load() == 0 {
			idleMirrors = append(idleMirrors, m)
		}
	}

	// Use idle pool if available, otherwise use all from current pool
	if len(idleMirrors) > 0 {
		selectionPool = idleMirrors
	}

	if len(selectionPool) == 1 {
		return selectionPool[0]
	}

	// [Enhanced] Weighted Random Selection with Success Rate
	// Score = (1/latency) * weight * successRate
	var totalScore float64
	type scoredMirror struct {
		m     *Mirror
		limit float64
	}
	var scoredPool []scoredMirror

	for _, m := range selectionPool {
		_, latency, _, reqs := m.GetStats()
		successRate := m.GetSuccessRate()

		// Base score from latency (inverse, in ms)
		latencyScore := 1.0 / (float64(latency+1) / float64(time.Millisecond))

		// Success rate factor: penalize mirrors with < 80% success heavily
		successFactor := 0.5 + 0.5*successRate // Range: 0.5 (0% success) to 1.0 (100% success)

		// Weighted score combining latency, success rate, and configured weight
		score := latencyScore * successFactor * float64(m.Weight)

		// Additional penalty for high failure count in current burst
		failCount := int(m.failCount.Load())
		if failCount > 0 {
			score *= math.Pow(0.8, float64(failCount)) // Exponential penalty per recent failure
		}

		totalScore += score
		scoredPool = append(scoredPool, scoredMirror{m: m, limit: totalScore})

		// Debug logging for selection process
		if reqs > 10 && successRate < 0.5 {
			log.Printf("[MIRROR] Low success rate: %s (%.1f%% success over %d reqs, score=%.2f)",
				m.URL, successRate*100, reqs, score)
		}
	}

	if totalScore == 0 {
		return selectionPool[0]
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
		errStr := strings.ToLower(err.Error())

		// DO NOT penalize the mirror for API rejections (Track unavailable, Preview, 400, 401, 403, 404)
		// Also don't penalize for context canceled (when SHOTGUN cancels pending requests after finding winner)
		// 429 is rate limit, so we DO penalize for 429 to trigger cooldown
		if strings.Contains(errStr, "preview") ||
			(strings.Contains(errStr, "400") && !strings.Contains(errStr, "429")) ||
			strings.Contains(errStr, "401") ||
			strings.Contains(errStr, "403") ||
			strings.Contains(errStr, "404") ||
			strings.Contains(errStr, "context canceled") {
			// Es una respuesta sana, Tidal simplemente denegó la pista o SHOTGUN canceló la petición
			// Actualizamos el success rate para que el mirror no pierda puntaje
			mm.updateSuccessRate(m, true)
			m.lastSuccess.Store(time.Now().Unix())
			return
		}

		failCount := int(m.failCount.Add(1))
		m.lastFail.Store(time.Now().Unix())

		isRateLimit := strings.Contains(errStr, "429")
		shouldCircuitBreak := isRateLimit || failCount >= mm.failureThreshold
		if shouldCircuitBreak && m.GetState() == StateHealthy {
			m.SetState(StateUnhealthy)
			reason := "multiple failures"
			if isRateLimit {
				reason = "rate limited (429)"
			}
			log.Printf("[MIRROR] %s marked unhealthy: %s (failCount=%d)", m.URL, reason, failCount)
		} else if m.GetState() == StateHealthy {
			// Truncate error message to avoid log spam from HTML responses
			maxErrLen := 200
			if len(errStr) > maxErrLen {
				errStr = errStr[:maxErrLen] + "... [truncated]"
			}
			log.Printf("[MIRROR] %s server error (will count toward threshold): %s (failCount=%d/%d)",
				m.URL, errStr, failCount, mm.failureThreshold)
		}
		return
	}

	// Success - update latency EMA
	m.lastSuccess.Store(time.Now().Unix())
	prevFailCount := int(m.failCount.Swap(0))

	oldEMA := m.latencyEMA.Load()
	newSample := int64(latency)

	newEMA := int64(0.7*float64(oldEMA) + 0.3*float64(newSample))
	m.latencyEMA.Store(newEMA)

	mm.updateSuccessRate(m, true)

	state := m.GetState()
	switch state {
	case StateUnhealthy:
		lastFail := time.Unix(m.lastFail.Load(), 0)
		if time.Since(lastFail) > mm.cooldownDuration {
			m.SetState(StateProbing)
			log.Printf("[MIRROR] %s recovered to probing (cooldown passed, success after %d failures)",
				m.URL, prevFailCount)
		}
	case StateProbing:
		m.SetState(StateHealthy)
		log.Printf("[MIRROR] %s recovered and marked healthy (latency: %v)", m.URL, latency)
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

// GetSuccessRate returns the success rate (0.0 to 1.0) based on recent requests
func (m *Mirror) GetSuccessRate() float64 {
	total := m.requestCount.Load()
	if total == 0 {
		return 1.0 // Default to optimistic 100% if no data
	}
	success := m.successCount.Load()
	return float64(success) / float64(total)
}

// updateSuccessRate updates the success/failure counters
func (mm *MirrorManager) updateSuccessRate(m *Mirror, success bool) {
	if success {
		m.successCount.Add(1)
	}
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

// checkMirror probes a single mirror verifying actual JSON responses
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

	// [HACKER FIX] Cambiar HEAD por GET para leer el cuerpo de la respuesta
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return
	}

	resp, err := client.Do(req)
	latency := time.Since(start)

	var isRealFailure bool
	if err != nil {
		isRealFailure = true
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		// 429 = Rate limited, 5xx = Server dead
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			isRealFailure = true
		} else if len(body) > 0 && body[0] == '<' {
			// Falso positivo: Nos devolvió un HTML de Cloudflare/Proxy en lugar de JSON
			isRealFailure = true
		}
	}

	if isRealFailure {
		m.lastHealthCheckFail.Store(time.Now().Unix())
		m.consecutiveSuccess.Store(0)
		if m.GetState() == StateHealthy {
			m.failCount.Add(1)
			if int(m.failCount.Load()) >= mm.failureThreshold {
				m.SetState(StateUnhealthy)
				log.Printf("[MIRROR] %s deep health check failed, marked unhealthy", m.URL)
			}
		}
		return
	}

	consecutive := m.consecutiveSuccess.Add(1)

	if m.GetState() == StateUnhealthy {
		lastHealthFail := time.Unix(m.lastHealthCheckFail.Load(), 0)
		if time.Since(lastHealthFail) > mm.cooldownDuration {
			m.SetState(StateProbing)
			m.consecutiveSuccess.Store(1)
			log.Printf("[MIRROR] %s cooldown ended, entering probing state", m.URL)
		}
	} else if m.GetState() == StateProbing {
		if consecutive >= 2 {
			m.SetState(StateHealthy)
			m.failCount.Store(0)
			log.Printf("[MIRROR] %s deep health check passed, marked healthy", m.URL)
		}
	} else {
		if consecutive > 100 {
			m.consecutiveSuccess.Store(100)
		}
	}

	mm.ReportResult(m, latency, nil)
}

// GetStatus returns full status for all mirrors (for debugging)
func (mm *MirrorManager) GetStatus() string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	status := "Mirror Status:\n"
	for _, m := range mm.mirrors {
		state, latency, fails, reqs := m.GetStats()
		status += fmt.Sprintf("  %s: %s | streamScore=%.1f | apiScore=%.1f | latency=%v | fails=%d | reqs=%d\n",
			m.URL, state, m.GetStreamScore(), m.GetAPIScore(), latency, fails, reqs)
	}
	return status
}

// GetAllMirrors devuelve todos los mirrors
func (mm *MirrorManager) GetAllMirrors() []*Mirror {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	mirrors := make([]*Mirror, len(mm.mirrors))
	copy(mirrors, mm.mirrors)
	return mirrors
}
