package deezer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.senan.xyz/gonic/db"
)

var (
	gwLightURL            = "https://www.deezer.com/ajax/gw-light.php"
	mediaURL              = "https://media.deezer.com/v1/get_url"
	defaultMediaCipherTag = string([]byte{0x42, 0x46, 0x5f, 0x43, 0x42, 0x43, 0x5f, 0x53, 0x54, 0x52, 0x49, 0x50, 0x45})
	muGatewayURLs         sync.RWMutex
)

func getGWLightURL() string {
	muGatewayURLs.RLock()
	defer muGatewayURLs.RUnlock()
	return gwLightURL
}

func getMediaURL() string {
	muGatewayURLs.RLock()
	defer muGatewayURLs.RUnlock()
	return mediaURL
}

// SetGatewayURLsForTest allows overriding Deezer endpoints during unit tests.
// Returns a restore function that reverts to previous URLs.
func SetGatewayURLsForTest(gw, media string) func() {
	muGatewayURLs.Lock()
	origGW := gwLightURL
	origMedia := mediaURL
	if gw != "" {
		gwLightURL = gw
	}
	if media != "" {
		mediaURL = media
	}
	muGatewayURLs.Unlock()

	return func() {
		muGatewayURLs.Lock()
		gwLightURL = origGW
		mediaURL = origMedia
		muGatewayURLs.Unlock()
	}
}

func setARLCookie(jar http.CookieJar, endpoint, arl string) {
	parsedURL, err := url.Parse(endpoint)
	if err != nil || parsedURL == nil {
		return
	}
	cookie := &http.Cookie{
		Name:     "arl",
		Value:    arl,
		Path:     "/",
		HttpOnly: true,
	}
	if parsedURL.Scheme == "https" {
		cookie.Secure = true
		if strings.HasSuffix(parsedURL.Hostname(), "deezer.com") {
			cookie.Domain = ".deezer.com"
		} else {
			cookie.Domain = parsedURL.Hostname()
		}
	}
	jar.SetCookies(parsedURL, []*http.Cookie{cookie})
}

// parseUserID extracts a string user ID from an interface{} value that may be float64 or string.
func parseUserID(val interface{}) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case float64:
		if v > 0 {
			return strconv.FormatInt(int64(v), 10)
		}
	case int:
		if v > 0 {
			return strconv.Itoa(v)
		}
	case int64:
		if v > 0 {
			return strconv.FormatInt(v, 10)
		}
	case string:
		clean := strings.TrimSpace(v)
		if clean != "" && clean != "0" {
			return clean
		}
	}
	return ""
}

// ValidateARL checks if an ARL cookie is valid against Deezer's internal API,
// returning subscriber identity, options, and subscription tier.
func ValidateARL(ctx context.Context, arl string) (*DeezerAccountInfo, error) {
	arl = strings.TrimSpace(arl)
	arl = strings.TrimPrefix(arl, "arl=")
	arl = strings.Trim(arl, "\";' ")
	arl = strings.TrimSpace(arl)
	if arl == "" {
		return nil, fmt.Errorf("empty ARL token")
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create cookie jar: %w", err)
	}

	setARLCookie(jar, getGWLightURL(), arl)

	client := &http.Client{
		Jar:     jar,
		Timeout: 15 * time.Second,
	}

	u := fmt.Sprintf("%s?method=deezer.getUserData&input=3&api_version=1.0&api_token=", getGWLightURL())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute getUserData: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var res GWUserDataResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("unmarshal getUserData response: %w", err)
	}

	userID := parseUserID(res.Results.User.UserID)
	if userID == "" || userID == "0" {
		return nil, fmt.Errorf("invalid ARL: user not authenticated or session expired")
	}

	username := strings.TrimSpace(res.Results.User.BlogName)
	if username == "" {
		username = fmt.Sprintf("Deezer User %s", userID)
	}

	subType := "FREE"
	highestQuality := "MP3_128"
	qualityTier := db.QualityTierFree

	if res.Results.User.Options.WebLossless {
		subType = "PREMIUM"
		highestQuality = "FLAC"
		qualityTier = db.QualityTierLossless
	} else if res.Results.User.Options.WebHQ {
		subType = "PREMIUM"
		highestQuality = "MP3_320"
		qualityTier = db.QualityTierHigh
	}

	return &DeezerAccountInfo{
		UserID:           userID,
		Username:         username,
		SubscriptionType: subType,
		HighestQuality:   highestQuality,
		QualityTier:      qualityTier,
		LicenseToken:     res.Results.User.Options.LicenseToken,
		CheckForm:        res.Results.CheckForm,
		WebLossless:      res.Results.User.Options.WebLossless,
		WebHQ:            res.Results.User.Options.WebHQ,
	}, nil
}

// AccountSession manages authenticated session state for a single Deezer account.
type AccountSession struct {
	Account *db.DeezerAccount

	mu            sync.Mutex
	httpClient    *http.Client
	licenseToken  string
	checkForm     string
	webLossless   bool
	webHQ         bool
	lastRefreshed time.Time
}

// NewAccountSession creates a session for an account.
func NewAccountSession(acc *db.DeezerAccount) *AccountSession {
	jar, _ := cookiejar.New(nil)
	setARLCookie(jar, getGWLightURL(), strings.TrimSpace(acc.ARL))

	return &AccountSession{
		Account: acc,
		httpClient: &http.Client{
			Jar:     jar,
			Timeout: 20 * time.Second,
		},
	}
}

// EnsureSession verifies that license_token and checkForm are valid, refreshing if needed.
func (s *AccountSession) EnsureSession(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.licenseToken != "" && s.checkForm != "" && time.Since(s.lastRefreshed) < 6*time.Hour {
		return nil
	}

	u := fmt.Sprintf("%s?method=deezer.getUserData&input=3&api_version=1.0&api_token=", getGWLightURL())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build getUserData request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request getUserData: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read getUserData: %w", err)
	}

	var res GWUserDataResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return fmt.Errorf("decode getUserData: %w", err)
	}

	userID := parseUserID(res.Results.User.UserID)
	if userID == "" || userID == "0" {
		return fmt.Errorf("deezer session expired for account %s (ID %d)", s.Account.Username, s.Account.ID)
	}

	s.licenseToken = res.Results.User.Options.LicenseToken
	s.checkForm = res.Results.CheckForm
	s.webLossless = res.Results.User.Options.WebLossless
	s.webHQ = res.Results.User.Options.WebHQ
	s.lastRefreshed = time.Now()

	if res.Results.User.BlogName != "" && s.Account.Username == "" {
		s.Account.Username = res.Results.User.BlogName
	}
	s.Account.UserID = userID

	if s.Account != nil && s.Account.QualityTier >= db.QualityTierLossless && !s.webLossless {
		return fmt.Errorf("account degraded: WebLossless=false for user %s", s.Account.Username)
	}

	return nil
}

// GetTrackToken calls song.getData to retrieve the track token for a specific song ID.
func (s *AccountSession) GetTrackToken(ctx context.Context, trackID int64) (*GWSongDataResponse, error) {
	if err := s.EnsureSession(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	checkForm := s.checkForm
	s.mu.Unlock()

	payload, err := json.Marshal(map[string]string{
		"sng_id": strconv.FormatInt(trackID, 10),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal song.getData payload: %w", err)
	}

	u := fmt.Sprintf("%s?method=song.getData&input=3&api_version=1.0&api_token=%s", getGWLightURL(), url.QueryEscape(checkForm))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build song.getData request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do song.getData: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read song.getData body: %w", err)
	}

	var res GWSongDataResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("decode song.getData: %w", err)
	}

	if res.Results.TrackToken == "" {
		// If CSRF expired, invalidate checkForm and retry once
		if strings.Contains(string(body), "VALID_TOKEN_REQUIRED") {
			s.mu.Lock()
			s.checkForm = ""
			s.mu.Unlock()
			if rErr := s.EnsureSession(ctx); rErr == nil {
				return s.GetTrackToken(ctx, trackID)
			}
		}
		return nil, fmt.Errorf("empty track token for track %d: %s", trackID, string(body))
	}

	return &res, nil
}

// ResetSession clears cached license tokens and CSRF form check tokens.
func (s *AccountSession) ResetSession() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.licenseToken = ""
	s.checkForm = ""
	s.lastRefreshed = time.Time{}
}

// GetMediaURL requests media.deezer.com/v1/get_url with the track token and returns the direct CDN URL and chosen format.
func (s *AccountSession) GetMediaURL(ctx context.Context, trackToken string, quality string) (string, string, error) {
	return s.getMediaURLInternal(ctx, trackToken, quality, true)
}

func (s *AccountSession) getMediaURLInternal(ctx context.Context, trackToken string, quality string, retryOnAuth bool) (string, string, error) {
	if err := s.EnsureSession(ctx); err != nil {
		return "", "", err
	}

	s.mu.Lock()
	licenseToken := s.licenseToken
	webLossless := s.webLossless
	s.mu.Unlock()

	// Build format priority based on user quality request and account capabilities
	var formats []FormatSpec
	reqQuality := strings.ToUpper(strings.TrimSpace(quality))

	switch reqQuality {
	case "LOW":
		formats = []FormatSpec{
			{Cipher: defaultMediaCipherTag, Format: "MP3_128"},
		}
	case "HIGH":
		formats = []FormatSpec{
			{Cipher: defaultMediaCipherTag, Format: "MP3_320"},
			{Cipher: defaultMediaCipherTag, Format: "MP3_128"},
		}
	default:
		// Normal / Lossless / FLAC requested
		if webLossless {
			formats = []FormatSpec{
				{Cipher: defaultMediaCipherTag, Format: "FLAC"},
				{Cipher: defaultMediaCipherTag, Format: "MP3_320"},
				{Cipher: defaultMediaCipherTag, Format: "MP3_128"},
			}
		} else {
			formats = []FormatSpec{
				{Cipher: defaultMediaCipherTag, Format: "MP3_320"},
				{Cipher: defaultMediaCipherTag, Format: "MP3_128"},
			}
		}
	}

	reqBody := MediaGetURLRequest{
		LicenseToken: licenseToken,
		Media: []MediaFormatReq{
			{
				Type:    "FULL",
				Formats: formats,
			},
		},
		TrackTokens: []string{trackToken},
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", fmt.Errorf("marshal get_url payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getMediaURL(), bytes.NewReader(jsonBytes))
	if err != nil {
		return "", "", fmt.Errorf("build get_url request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("do get_url: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("read get_url body: %w", err)
	}

	var mediaResp MediaGetURLResponse
	if err := json.Unmarshal(body, &mediaResp); err != nil {
		return "", "", fmt.Errorf("decode get_url response: %w", err)
	}

	if len(mediaResp.Errors) > 0 {
		// If license token expired or invalid (code 1000), invalidate and retry once with fresh session
		if retryOnAuth && mediaResp.Errors[0].Code == 1000 {
			s.ResetSession()
			if rErr := s.EnsureSession(ctx); rErr == nil {
				return s.getMediaURLInternal(ctx, trackToken, quality, false)
			}
		}
		return "", "", fmt.Errorf("media.deezer.com error: %s (code %d)", mediaResp.Errors[0].Message, mediaResp.Errors[0].Code)
	}

	for _, item := range mediaResp.Data {
		for _, m := range item.Media {
			for _, src := range m.Sources {
				if src.URL != "" {
					return src.URL, m.Format, nil
				}
			}
		}
		if len(item.Errors) > 0 {
			return "", "", fmt.Errorf("track media error: %s (code %d)", item.Errors[0].Message, item.Errors[0].Code)
		}
	}

	return "", "", fmt.Errorf("no stream URL found in media.deezer.com response: %s", string(body))
}

// AccountPool maintains active Deezer subscriber accounts and performs round-robin rotation.
type AccountPool struct {
	mu                sync.RWMutex
	sessions          []*AccountSession
	rrIndex           uint64
	db                *db.DB
	onAccountDegraded func(accountID int, reason string)
}

// NewAccountPool creates an empty AccountPool.
func NewAccountPool(dbc *db.DB) *AccountPool {
	return &AccountPool{
		db: dbc,
	}
}

// SetDB sets or updates the database reference.
func (p *AccountPool) SetDB(dbc *db.DB) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.db = dbc
}

// SetAccountDegradedHandler sets a callback invoked when an account is demoted due to degraded subscription.
func (p *AccountPool) SetAccountDegradedHandler(fn func(accountID int, reason string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onAccountDegraded = fn
}

// SyncAccounts synchronizes active accounts with the database, preserving alive sessions where possible.
func (p *AccountPool) SyncAccounts(accounts []*db.DeezerAccount) {
	p.mu.Lock()
	defer p.mu.Unlock()

	existing := make(map[int]*AccountSession)
	for _, s := range p.sessions {
		if s.Account != nil {
			existing[s.Account.ID] = s
		}
	}

	var updated []*AccountSession
	for _, acc := range accounts {
		if !acc.IsActive {
			continue
		}
		if prev, ok := existing[acc.ID]; ok && prev.Account.ARL == acc.ARL {
			prev.Account = acc
			updated = append(updated, prev)
		} else {
			updated = append(updated, NewAccountSession(acc))
		}
	}

	p.sessions = updated
	log.Printf("[DEEZER] Account pool synchronized with %d active accounts", len(updated))
}

// GetNextAccount selects the next available account meeting the requested minTier using round-robin.
func (p *AccountPool) GetNextAccount(minTier int) (*AccountSession, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.sessions)
	if n == 0 {
		return nil, fmt.Errorf("no active Deezer accounts available in pool")
	}

	// 1. Filter sessions meeting quality tier
	var eligible []*AccountSession
	for _, s := range p.sessions {
		if s.Account.IsActive && s.Account.QualityTier >= minTier {
			eligible = append(eligible, s)
		}
	}

	// 2. Fallback to any active account if no tier match
	if len(eligible) == 0 {
		for _, s := range p.sessions {
			if s.Account.IsActive {
				eligible = append(eligible, s)
			}
		}
	}

	if len(eligible) == 0 {
		return nil, fmt.Errorf("no active Deezer accounts configured")
	}

	idx := atomic.AddUint64(&p.rrIndex, 1) % uint64(len(eligible))
	return eligible[idx], nil
}

// MarkError records an error on an account and persists it to the database if available.
func (p *AccountPool) MarkError(accountID int, err error) {
	if err == nil {
		return
	}
	// Ignore client-side cancellations (e.g. user skipped song or closed player)
	if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
		return
	}
	errStr := err.Error()
	degraded, isFatalAuth, reason := isDeezerSubscriptionDegraded(err, "")

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
				s.Account.HighestQuality = "MP3_128"
				s.Account.IsActive = false
			}
			break
		}
	}
	p.mu.Unlock()

	if p.db != nil {
		_ = p.db.UpdateDeezerAccountError(accountID, errStr)
		if demoted {
			log.Printf("[DEEZER] ⛔ Demoting account id=%d to FREE — reason: %s", accountID, reason)
			_ = p.db.MarkDeezerAccountAsFree(accountID)
		} else if deactivated {
			log.Printf("[DEEZER] ⛔ Deactivating account id=%d — reason: %s", accountID, reason)
			_ = p.db.ToggleDeezerAccount(accountID, false)
		}
	}

	if demoted && degradedHandler != nil {
		degradedHandler(accountID, reason)
	}
}

// ResetAccountSession resets cached tokens for an account.
func (p *AccountPool) ResetAccountSession(accountID int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.sessions {
		if s.Account != nil && s.Account.ID == accountID {
			s.ResetSession()
			break
		}
	}
}

// Count returns total active sessions in the pool.
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

// HasLosslessAccount returns true if any active account in the pool supports FLAC (QualityTierLossless or higher).
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


