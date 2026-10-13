package ctrladmin

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"image"
	"image/jpeg"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/sessions"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/listenbrainz"
	"go.senan.xyz/gonic/provider/deezer"
	"go.senan.xyz/gonic/provider/qobuz"
	"go.senan.xyz/gonic/provider/tidal"
	"go.senan.xyz/gonic/provider/ytmusic"
)

func (c *Controller) ServeNotFound(_ *http.Request) *Response {
	return &Response{template: "not_found.tmpl", code: 404}
}

func (c *Controller) ServeLogin(r *http.Request) *Response {
	// Check if user is already logged in
	session := r.Context().Value(CtxSession).(*sessions.Session)
	if _, ok := session.Values["user"].(int); ok {
		// User is already authenticated, redirect to profile
		return &Response{redirect: "/admin/profile"}
	}
	return &Response{template: "login.tmpl"}
}

func (c *Controller) ServeHome(r *http.Request) *Response {
	user := r.Context().Value(CtxUser).(*db.User)

	data := &templateData{}
	// stats box - global counts across all users
	albums, artists, tracks := c.dbc.GetGlobalStats()

	data.Stats = StatsData{
		Albums:  albums,
		Artists: artists,
		Tracks:  tracks,
	}

	// cache stats from all levels
	data.CacheStats = c.proxy.Stats()

	data.RequestRoot = handlerutil.BaseURL(r)
	data.DefaultListenBrainzURL = listenbrainz.BaseURL

	// users box
	allUsersQ := c.dbc.DB
	if !user.IsAdmin {
		allUsersQ = allUsersQ.Where("name=?", user.Name)
	}
	allUsersQ.Find(&data.AllUsers)

	// Last.fm API key configuration (visible to admins in template)
	data.CurrentLastFMAPIKey = c.dbc.GetSetting("lastfm_api_key", "")
	data.CurrentLastFMAPISecret = c.dbc.GetSetting("lastfm_api_secret", "")

	return &Response{
		template: "home.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeRoot(r *http.Request) *Response {
	// Check if user is authenticated
	if user, ok := r.Context().Value(CtxUser).(*db.User); ok && user != nil {
		// Redirect to profile for authenticated users
		return &Response{redirect: "/admin/profile"}
	}
	// Redirect to login for unauthenticated users
	return &Response{redirect: "/admin/login"}
}

func (c *Controller) ServeProfile(r *http.Request) *Response {
	user := r.Context().Value(CtxUser).(*db.User)

	// Get user stats
	albums, artists, tracks := c.dbc.GetUserStats(user.ID)

	// Get total play count
	var totalPlays int64
	c.dbc.Model(&db.Play{}).Where("user_id = ?", user.ID).Select("SUM(count)").Row().Scan(&totalPlays)

	// Check if Last.fm API key is configured (enables OAuth flow)
	lastfmAPIKey := c.dbc.GetSetting("lastfm_api_key", "")

	data := &templateData{
		SelectedUser:           user,
		Stats:                  StatsData{Albums: albums, Artists: artists, Tracks: tracks},
		DefaultListenBrainzURL: listenbrainz.BaseURL,
		CurrentLastFMAPIKey:    lastfmAPIKey,
	}

	// For play count, we'll add a custom field to templateData or use a workaround
	_ = totalPlays

	return &Response{
		template: "profile.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeUnlinkLastFMDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/profile", flashW: []string{"invalid security token"}}
	}

	user := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)
	user.LastfmSession = ""
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	log.Printf("[AUDIT] Last.fm unlinked for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/profile", flashN: []string{"last.fm unlinked successfully"}}
}

// ServeLinkLastFMStart initiates Last.fm OAuth flow or shows manual entry form
// If server has Last.fm API key configured, redirects to Last.fm auth page
// Otherwise, shows the manual session key entry form
func (c *Controller) ServeLinkLastFMStart(r *http.Request) *Response {
	apiKey := c.dbc.GetSetting("lastfm_api_key", "")

	// If API key is configured, redirect to Last.fm OAuth
	if apiKey != "" {
		// Build callback URL - use the same host as the request
		callbackURL := handlerutil.BaseURL(r) + "/admin/link_lastfm_callback"

		// Redirect to Last.fm authorization page
		authURL := fmt.Sprintf("https://www.last.fm/api/auth?api_key=%s&cb=%s",
			url.QueryEscape(apiKey),
			url.QueryEscape(callbackURL))

		return &Response{
			redirect: authURL,
		}
	}

	// No API key configured - show manual entry form
	user := r.Context().Value(CtxUser).(*db.User)
	data := &templateData{
		SelectedUser: user,
	}

	return &Response{
		template: "link_lastfm.tmpl",
		data:     data,
	}
}

// ServeLinkLastFMCallback handles both OAuth callback and manual session entry
// lastFMSessionRegex validates Last.fm session keys (32 hex characters, MD5 hash)
var lastFMSessionRegex = regexp.MustCompile("^[a-fA-F0-9]{32}$")

func (c *Controller) ServeLinkLastFMCallback(r *http.Request) *Response {
	// Check for OAuth token from Last.fm callback (GET request)
	oauthToken := r.URL.Query().Get("token")

	if oauthToken != "" {
		// OAuth flow: exchange token for session key
		return c.handleLastFMOAuthCallback(r, oauthToken)
	}

	// Manual session entry flow (POST request with form)
	// Validate CSRF token for form submissions
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/profile", flashW: []string{"invalid security token"}}
	}

	session := strings.TrimSpace(r.FormValue("session"))
	if session == "" {
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"please provide a last.fm session key"},
		}
	}

	// Validate session key format (must be 32 hex characters)
	// Last.fm session keys are MD5 hashes: 32 hexadecimal characters
	if !lastFMSessionRegex.MatchString(session) {
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"invalid session key format (must be 32 hexadecimal characters)"},
		}
	}

	user := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)
	user.LastfmSession = session
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: "/admin/profile", flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	log.Printf("[AUDIT] Last.fm linked for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/profile", flashN: []string{"last.fm linked successfully"}}
}

// handleLastFMOAuthCallback exchanges an OAuth token for a session key
func (c *Controller) handleLastFMOAuthCallback(r *http.Request, token string) *Response {
	apiKey := c.dbc.GetSetting("lastfm_api_key", "")
	apiSecret := c.dbc.GetSetting("lastfm_api_secret", "")

	if apiKey == "" || apiSecret == "" {
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"Last.fm API key not configured. Please contact your administrator."},
		}
	}

	// Build request to exchange token for session
	params := url.Values{}
	params.Add("method", "auth.getSession")
	params.Add("api_key", apiKey)
	params.Add("token", token)
	params.Add("api_sig", c.getLastFMSignature(params, apiSecret))

	apiURL := "https://ws.audioscrobbler.com/2.0/?" + params.Encode()

	resp, err := http.Get(apiURL)
	if err != nil {
		log.Printf("[ERROR] Failed to exchange Last.fm token: %v", err)
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"failed to link Last.fm account: API error"},
		}
	}
	defer resp.Body.Close()

	var lfmlastfm LastFMSessionResponse
	if err := xml.NewDecoder(resp.Body).Decode(&lfmlastfm); err != nil {
		log.Printf("[ERROR] Failed to parse Last.fm response: %v", err)
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"failed to link Last.fm account: invalid response"},
		}
	}

	if lfmlastfm.Error.Code != 0 {
		log.Printf("[ERROR] Last.fm API error: %s (code %d)", lfmlastfm.Error.Value, lfmlastfm.Error.Code)
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{fmt.Sprintf("Last.fm error: %s", lfmlastfm.Error.Value)},
		}
	}

	session := lfmlastfm.Session.Key
	if session == "" {
		return &Response{
			redirect: "/admin/profile",
			flashW:   []string{"failed to link Last.fm account: no session received"},
		}
	}

	user := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)
	user.LastfmSession = session
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: "/admin/profile", flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	log.Printf("[AUDIT] Last.fm linked via OAuth for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/profile", flashN: []string{"Last.fm account linked successfully via OAuth"}}
}

// getLastFMSignature generates a Last.fm API signature
func (c *Controller) getLastFMSignature(params url.Values, secret string) string {
	// Parameters must be in alphabetical order before hashing
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	toHash := ""
	for _, k := range keys {
		toHash += k
		toHash += params[k][0]
	}
	toHash += secret

	hash := md5.Sum([]byte(toHash))
	return hex.EncodeToString(hash[:])
}

// LastFMSessionResponse represents the auth.getSession response from Last.fm
type LastFMSessionResponse struct {
	Session struct {
		Key        string `xml:"key"`
		Subscriber string `xml:"subscriber"`
		Name       string `xml:"name"`
	} `xml:"session"`
	Error struct {
		Code  int    `xml:"code,attr"`
		Value string `xml:",chardata"`
	} `xml:"error"`
}

func (c *Controller) ServeLinkListenBrainzDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/profile", flashW: []string{"invalid security token"}}
	}

	token := strings.TrimSpace(r.FormValue("token"))
	urlStr := strings.TrimSpace(r.FormValue("url"))
	if token == "" || urlStr == "" {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/profile"),
			flashW:   []string{"please provide a url and token"},
		}
	}

	// Validate token length
	if len(token) > 256 {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"invalid token format"}}
	}

	// Strict URL validation - must be valid HTTP/HTTPS URL
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"invalid URL format"}}
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"URL must use http or https scheme"}}
	}
	if parsedURL.Host == "" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"URL must have a valid host"}}
	}
	// Reject URLs with userinfo (credentials)
	if parsedURL.User != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"URL must not contain credentials"}}
	}
	// Enforce length limits after parsing
	if len(urlStr) > 256 {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{"URL too long"}}
	}

	user := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)
	user.ListenbrainzUrl = parsedURL.String()
	user.ListenbrainzToken = token
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	log.Printf("[AUDIT] ListenBrainz linked for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/profile", flashN: []string{"listenbrainz linked successfully"}}
}

func (c *Controller) ServeUnlinkListenBrainzDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/profile", flashW: []string{"invalid security token"}}
	}

	user := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)
	user.ListenbrainzUrl = ""
	user.ListenbrainzToken = ""
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	log.Printf("[AUDIT] ListenBrainz unlinked for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/profile", flashN: []string{"listenbrainz unlinked successfully"}}
}

func (c *Controller) ServeChangeUsername(r *http.Request) *Response {
	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin changing another user's username
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own username
		user = r.Context().Value(CtxUser).(*db.User)
	}

	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_username.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangeUsernameDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin changing another user's username
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own username
		user = r.Context().Value(CtxUser).(*db.User)
	}

	// Get the acting user
	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// Check rate limit for sensitive operations
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":username"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for username change by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}

	// Require current password for security (prevents session hijacking username changes)
	currentPassword := r.FormValue("current_password")

	// If admin is changing another user's username, verify ADMIN's password
	// If user is changing their own username, verify THEIR password
	passwordToVerify := user.Password
	if actingUser.ID != user.ID {
		// Admin changing another user - verify admin's password
		passwordToVerify = actingUser.Password
	}

	if !verifyPassword(passwordToVerify, currentPassword) {
		log.Printf("[AUDIT] Failed username change attempt for user %s by %s from %s: incorrect password", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"current password is incorrect"},
		}
	}

	usernameNew := r.FormValue("username")
	oldUsername := user.Name
	if err := validateUsername(usernameNew); err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{err.Error()},
		}
	}

	// Prevent changing to an existing username
	existingUser := c.dbc.GetUserByName(usernameNew)
	if existingUser != nil && existingUser.ID != user.ID {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"username already taken"},
		}
	}

	// CRITICAL: Re-verify acting user is still admin before saving (prevents race condition)
	if actingUser.ID != user.ID {
		freshActingUser := c.dbc.GetUserByID(actingUser.ID)
		if freshActingUser == nil || !freshActingUser.IsAdmin {
			log.Printf("[SECURITY] Admin privilege revocation detected during username change for user %s by %s", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name))
			return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
		}
	}

	user.Name = usernameNew
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{fmt.Sprintf("save username: %v", err)}}
	}

	log.Printf("[AUDIT] Username changed from %s to %s by %s from %s", sanitizeLogValue(oldUsername), sanitizeLogValue(usernameNew), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/home", flashN: []string{fmt.Sprintf("username changed to %s", usernameNew)}}
}

func (c *Controller) ServeChangePassword(r *http.Request) *Response {
	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin changing another user's password
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own password
		user = r.Context().Value(CtxUser).(*db.User)
	}

	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_password.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangePasswordDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername == "" {
		selectedUsername = r.FormValue("user")
	}
	if selectedUsername != "" {
		// Admin changing another user's password
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own password
		user = r.Context().Value(CtxUser).(*db.User)
	}

	// Get the acting user (for verification)
	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// Check rate limit for sensitive operations
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":password"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for password change by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}

	// Require current password for security when changing own password
	// Skip verification when admin resets another user's password (user forgot password scenario)
	if actingUser.ID == user.ID {
		currentPassword := r.FormValue("current_password")
		if !verifyPassword(user.Password, currentPassword) {
			log.Printf("[AUDIT] Failed password change attempt for user %s from %s: incorrect current password", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
			return &Response{
				redirect: safeRedirectPath(r.Referer(), "/admin/profile"),
				flashW:   []string{"current password is incorrect"},
			}
		}
	}

	passwordOne := r.FormValue("password_one")
	passwordTwo := r.FormValue("password_two")
	if err := validatePasswords(passwordOne, passwordTwo); err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{err.Error()},
		}
	}

	// Hash the new password before saving
	hashedPassword, err := hashPassword(passwordOne)
	if err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{"failed to hash password"}}
	}

	// CRITICAL: Re-verify acting user is still admin before saving (prevents race condition where admin loses privileges)
	if actingUser.ID != user.ID {
		freshActingUser := c.dbc.GetUserByID(actingUser.ID)
		if freshActingUser == nil || !freshActingUser.IsAdmin {
			log.Printf("[SECURITY] Admin privilege revocation detected during password change for user %s by %s", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name))
			return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
		}
	}

	user.Password = hashedPassword
	user.SubsonicPassword = passwordOne // Plaintext for Subsonic token auth
	if err := c.dbc.Save(user).Error; err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/profile"), flashW: []string{fmt.Sprintf("save user: %v", err)}}
	}

	// Invalidate all sessions for the user whose password changed (forces re-login with new password)
	// IMPORTANT: We must update the current session's session_version to match, otherwise
	// this session will be rejected on the next request (session version mismatch)
	if err := c.invalidateUserSessions(user.ID); err != nil {
		log.Printf("[WARN] Failed to invalidate sessions for user %d: %v", user.ID, err)
	}

	targetUser := user.Name
	if actingUser.ID != user.ID {
		targetUser = fmt.Sprintf("%s (admin action on %s)", actingUser.Name, user.Name)
	}
	log.Printf("[AUDIT] Password changed for user %s by %s from %s", sanitizeLogValue(targetUser), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))

	// Regenerate CSRF token on password change (security best practice)
	session := r.Context().Value(CtxSession).(*sessions.Session)
	session.Values["csrf_token"] = generateCSRFToken()
	// Update session_version to match the incremented value in database
	// This prevents immediate session invalidation on next request
	session.Values["session_version"] = user.SessionVersion + 1

	// If user changed their own password, redirect to profile
	if actingUser.ID == user.ID {
		return &Response{redirect: "/admin/profile", flashN: []string{"password changed successfully"}}
	}
	return &Response{redirect: "/admin/home", flashN: []string{fmt.Sprintf("password changed for %s", user.Name)}}
}

func (c *Controller) ServeChangeAvatar(r *http.Request) *Response {
	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin changing another user's avatar
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own avatar
		user = r.Context().Value(CtxUser).(*db.User)
	}

	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "change_avatar.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeChangeAvatarDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin changing another user's avatar
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User changing their own avatar
		user = r.Context().Value(CtxUser).(*db.User)
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// Check rate limit for file uploads (prevent storage DoS)
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":avatar_upload"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for avatar upload by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}

	avatar, err := getAvatarFile(r)
	if err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{err.Error()},
		}
	}

	user.Avatar = avatar
	if err := c.dbc.Save(user).Error; err != nil {
		log.Printf("[ERROR] Failed to save user avatar: %v", err)
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{"could not save avatar"}}
	}

	if actingUser.ID != user.ID {
		log.Printf("[AUDIT] Avatar changed for user %s by admin %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	} else {
		log.Printf("[AUDIT] Avatar changed for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	}

	return &Response{
		redirect: safeRedirectPath(r.Referer(), "/admin/home"),
		flashN:   []string{"avatar saved successfully"},
	}
}

func (c *Controller) ServeDeleteAvatarDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	var user *db.User
	var err error

	// Check if user parameter is provided
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername != "" {
		// Admin deleting another user's avatar
		user, err = selectedUserIfAdmin(c, r)
		if err != nil {
			return &Response{code: 400, err: err.Error()}
		}
	} else {
		// User deleting their own avatar
		user = r.Context().Value(CtxUser).(*db.User)
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	user.Avatar = nil
	if err := c.dbc.Save(user).Error; err != nil {
		log.Printf("[ERROR] Failed to delete user avatar: %v", err)
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{"could not delete avatar"}}
	}

	if actingUser.ID != user.ID {
		log.Printf("[AUDIT] Avatar deleted for user %s by admin %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	} else {
		log.Printf("[AUDIT] Avatar deleted for user %s from %s", sanitizeLogValue(user.Name), sanitizeLogValue(clientIP))
	}

	return &Response{
		redirect: safeRedirectPath(r.Referer(), "/admin/home"),
		flashN:   []string{"avatar deleted successfully"},
	}
}

func (c *Controller) ServeDeleteUser(r *http.Request) *Response {
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	data := &templateData{}
	data.SelectedUser = user
	return &Response{
		template: "delete_user.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeDeleteUserDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}
	user, err := selectedUserIfAdmin(c, r)
	if err != nil {
		return &Response{code: 400, err: err.Error()}
	}
	if user.IsAdmin {
		// Check if this is the last admin - we can't delete the last admin or we'll lock ourselves out
		adminCount := c.dbc.AdminCount()
		if adminCount <= 1 {
			log.Printf("[SECURITY] Attempted to delete the last admin user %s - operation blocked", sanitizeLogValue(user.Name))
			return &Response{
				redirect: "/admin/home",
				flashW:   []string{"cannot delete the last admin user"},
			}
		}
		return &Response{
			redirect: "/admin/home",
			flashW:   []string{"can't delete an admin user"},
		}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// Check rate limit for sensitive operations
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":delete_user"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for user deletion by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}

	// Require current password for security (prevents session hijacking user deletion)
	currentPassword := r.FormValue("current_password")

	// If admin is deleting another user, verify ADMIN's password
	// If user is deleting themselves, verify THEIR password
	passwordToVerify := user.Password
	if actingUser.ID != user.ID {
		// Admin deleting another user - verify admin's password
		passwordToVerify = actingUser.Password
	}

	if !verifyPassword(passwordToVerify, currentPassword) {
		log.Printf("[AUDIT] Failed user deletion attempt for user %s by %s from %s: incorrect password", sanitizeLogValue(user.Name), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"current password is incorrect"},
		}
	}

	// Require confirmation text "DELETE" to prevent accidental deletions
	confirmation := r.FormValue("confirmation")
	if confirmation != "DELETE" {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"please type DELETE to confirm user deletion"},
		}
	}

	deletedUsername := user.Name

	if err := c.dbc.Delete(user).Error; err != nil {
		log.Printf("[ERROR] Failed to delete user: %v", err)
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{"could not delete user"}}
	}

	log.Printf("[AUDIT] User %s deleted by %s from %s", sanitizeLogValue(deletedUsername), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/home", flashN: []string{fmt.Sprintf("user %s deleted", deletedUsername)}}
}

func (c *Controller) ServeCreateUser(_ *http.Request) *Response {
	return &Response{template: "create_user.tmpl"}
}

func (c *Controller) ServeCreateUserDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// Check rate limit for user creation (prevents admin credential abuse)
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":create_user"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for user creation by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}
	username := r.FormValue("username")
	if err := validateUsername(username); err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{err.Error()},
		}
	}
	passwordOne := r.FormValue("password_one")
	passwordTwo := r.FormValue("password_two")
	if err := validatePasswords(passwordOne, passwordTwo); err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{err.Error()},
		}
	}

	// Check role selection
	roleVal := r.FormValue("role")
	log.Printf("[ADMIN] Creating user %s, role: %q", sanitizeLogValue(username), sanitizeLogValue(roleVal))
	isAdmin := roleVal == "admin"

	// Hash the password before storing
	hashedPassword, err := hashPassword(passwordOne)
	if err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"failed to hash password"},
		}
	}

	// Check for duplicate username
	existingUser := c.dbc.GetUserByName(username)
	if existingUser != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"username already exists"},
		}
	}

	// CRITICAL: Re-verify acting user is still admin before creating user (prevents race condition)
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during user creation by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	user := db.User{
		Name:             username,
		Password:         hashedPassword, // Bcrypt hash for secure auth
		SubsonicPassword: passwordOne,    // Plaintext for Subsonic token auth
		IsAdmin:          isAdmin,
	}
	if err := c.dbc.Create(&user).Error; err != nil {
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{fmt.Sprintf("could not create user %q: %v", username, err)},
		}
	}

	role := "user"
	if isAdmin {
		role = "admin"
	}
	log.Printf("[AUDIT] New %s %s created by %s from %s", sanitizeLogValue(role), sanitizeLogValue(username), sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))

	return &Response{
		redirect: "/admin/home",
		flashN:   []string{fmt.Sprintf("created %s %q successfully", role, username)},
	}
}

func getAvatarFile(r *http.Request) ([]byte, error) {
	err := r.ParseMultipartForm(10 << 20) // keep up to 10MB in memory
	if err != nil {
		return nil, err
	}
	file, header, err := r.FormFile("avatar")
	if err != nil {
		return nil, fmt.Errorf("read form file: %w", err)
	}
	defer file.Close()

	// CRITICAL: Validate file size from header BEFORE allocating buffer
	// This prevents OOM attacks with manipulated Content-Length headers
	if header.Size > 5<<20 { // 5MB max
		return nil, fmt.Errorf("avatar file too large (max 5MB)")
	}
	if header.Size <= 0 {
		return nil, fmt.Errorf("invalid avatar file size")
	}

	// Decode with dimension limits to prevent decompression bombs
	// Read into buffer first to allow multiple passes if needed
	data := make([]byte, header.Size)
	n, err := file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("read file data: %w", err)
	}
	data = data[:n]

	// Validate magic bytes to ensure it's actually an image file (defense in depth)
	// This prevents uploading disguised malicious files
	if len(data) < 8 {
		return nil, fmt.Errorf("file too small to be a valid image")
	}
	// Check for common image format magic bytes
	isValidImage := false
	// JPEG: FF D8 FF
	if data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		isValidImage = true
	}
	// PNG: 89 50 4E 47 0D 0A 1A 0A
	if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 {
		isValidImage = true
	}
	// GIF: GIF87a or GIF89a
	if data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x38 {
		isValidImage = true
	}
	// WebP: RIFF....WEBP
	if data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46 &&
		len(data) > 11 && data[8] == 0x57 && data[9] == 0x45 && data[10] == 0x42 && data[11] == 0x50 {
		isValidImage = true
	}
	if !isValidImage {
		return nil, fmt.Errorf("invalid image format (must be JPEG, PNG, GIF, or WebP)")
	}

	// Use image.DecodeConfig first to check dimensions without full decode
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("read image config: %w", err)
	}

	// Reject images with suspicious dimensions
	const maxDimension = 4096
	if config.Width > maxDimension || config.Height > maxDimension {
		return nil, fmt.Errorf("image dimensions too large (max %dx%d)", maxDimension, maxDimension)
	}
	if config.Width < 16 || config.Height < 16 {
		return nil, fmt.Errorf("image dimensions too small")
	}
	// Reject potentially malicious dimension ratios (decompression bomb indicator)
	if config.Width*config.Height > 50_000_000 { // 50 megapixels max
		return nil, fmt.Errorf("image pixel count too high")
	}

	// Now decode the full image
	i, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode image: %w", err)
	}

	// Re-encode as JPEG with moderate quality to normalize and strip metadata
	var buff bytes.Buffer
	if err := jpeg.Encode(&buff, i, &jpeg.Options{Quality: 85}); err != nil {
		return nil, err
	}

	// Final size check after re-encoding
	if buff.Len() > 2<<20 { // 2MB max after processing
		return nil, fmt.Errorf("processed avatar too large")
	}

	return buff.Bytes(), nil
}

func selectedUserIfAdmin(c *Controller, r *http.Request) (*db.User, error) {
	selectedUsername := r.URL.Query().Get("user")
	if selectedUsername == "" {
		selectedUsername = r.FormValue("user")
	}
	if selectedUsername == "" {
		return nil, fmt.Errorf("invalid user")
	}

	user := r.Context().Value(CtxUser).(*db.User)
	if !user.IsAdmin && user.Name != selectedUsername {
		return nil, fmt.Errorf("permission denied")
	}
	selectedUser := c.dbc.GetUserByName(selectedUsername)
	if selectedUser == nil {
		return nil, fmt.Errorf("user not found")
	}
	return selectedUser, nil
}
func (c *Controller) ServeProxies(r *http.Request) *Response {
	proxies, err := c.dbc.GetProxies()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get proxies: %v", err)}
	}

	data := &templateData{}
	data.Proxies = proxies

	// Get mirror stats from MirrorManager
	if cachedProxy, ok := c.proxy.(*tidal.CachedProxy); ok {
		if mgr := cachedProxy.GetMirrorManager(); mgr != nil {
			data.MirrorStats = mgr.GetStatus()
		}
	}

	return &Response{
		template: "proxies.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeAddProxyDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/proxies", flashW: []string{"invalid security token"}}
	}
	proxyURL := r.FormValue("url")
	name := r.FormValue("name")
	if proxyURL == "" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"URL is required"}}
	}

	// Validate proxy name length to prevent storage DoS
	if len(name) > 128 {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"proxy name too long (max 128 characters)"}}
	}

	// Validate URL format and scheme to prevent SSRF
	parsedURL, err := url.Parse(proxyURL)
	if err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"invalid URL format"}}
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"URL must use http or https scheme"}}
	}
	if parsedURL.Host == "" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"URL must have a valid host"}}
	}
	// Reject localhost and private IP ranges to prevent SSRF
	host := parsedURL.Hostname()
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "0.0.0.0" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"cannot use localhost as proxy URL"}}
	}

	// Check for private IP ranges AND resolve hostnames to prevent DNS rebinding SSRF
	// This ensures that even if a hostname is used, it doesn't resolve to internal IPs
	if resolveAndCheckPrivate(host) {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"cannot use private IP ranges or internal hostnames as proxy URL"}}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// CRITICAL: Re-verify acting user is still admin before adding proxy (prevents race condition)
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during proxy add by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	if err := c.dbc.AddProxy(proxyURL, name, "manual"); err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{fmt.Sprintf("add proxy: %v", err)}}
	}

	// update live pool
	if err := c.syncProxyPool(); err != nil {
		log.Printf("proxy: sync pool error after add: %v", err)
	}

	log.Printf("[AUDIT] Proxy added by %s from %s: %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP), sanitizeLogValue(proxyURL))
	return &Response{redirect: "/admin/proxies", flashN: []string{"proxy added"}}
}

func (c *Controller) ServeDeleteProxyDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/proxies", flashW: []string{"invalid security token"}}
	}
	idString := r.FormValue("id")
	if idString == "" {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"proxy ID is required"}}
	}
	idInt, err := strconv.Atoi(idString)
	if err != nil || idInt <= 0 {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{"invalid proxy ID"}}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// CRITICAL: Re-verify acting user is still admin before deleting proxy (prevents race condition)
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during proxy delete by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	if err := c.dbc.DeleteProxy(idInt); err != nil {
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/proxies"), flashW: []string{fmt.Sprintf("delete proxy: %v", err)}}
	}

	// update live pool
	if err := c.syncProxyPool(); err != nil {
		log.Printf("proxy: sync pool error after delete: %v", err)
	}

	log.Printf("[AUDIT] Proxy deleted (ID: %d) by %s from %s", idInt, sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/proxies", flashN: []string{"proxy deleted"}}
}

func (c *Controller) syncProxyPool() error {
	proxies, err := c.dbc.GetProxies()
	if err != nil {
		return err
	}
	urls := make([]string, len(proxies))
	for i, p := range proxies {
		urls[i] = p.URL
	}
	c.proxy.SetInstances(urls)
	return nil
}

func (c *Controller) ServeClearCacheDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// CRITICAL: Re-verify acting user is still admin before clearing cache (prevents race condition)
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during cache clear by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	// Check rate limit for expensive operations (prevent DoS via cache clearing)
	if allowed, msg := c.rateLimiter.CheckLimit(clientIP + ":clear_cache"); !allowed {
		log.Printf("[AUDIT] Rate limit hit for cache clear by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{redirect: safeRedirectPath(r.Referer(), "/admin/home"), flashW: []string{msg}}
	}

	// Clear SQLite metadata_cache
	if err := c.dbc.ClearAllCache(); err != nil {
		return &Response{redirect: "/admin/home", flashW: []string{fmt.Sprintf("clear sqlite cache: %v", err)}}
	}
	// Clear in-memory LRU caches
	c.proxy.ClearAll()

	log.Printf("[AUDIT] Cache cleared by %s from %s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
	return &Response{redirect: "/admin/home", flashN: []string{"all caches cleared successfully (SQLite + in-memory)"}}
}

// isPrivateOrReservedIP checks if an IP is in a private/reserved range
func isPrivateOrReservedIP(ip net.IP) bool {
	// Check for loopback
	if ip.IsLoopback() {
		return true
	}

	// Check for private ranges
	if ip.IsPrivate() {
		return true
	}

	// Check for link-local addresses
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	// Check for unspecified (0.0.0.0)
	if ip.IsUnspecified() {
		return true
	}

	return false
}

// resolveAndCheckPrivate resolves a hostname to IP and checks if it points to private addresses
// This prevents DNS rebinding attacks where a domain resolves to internal IPs
func resolveAndCheckPrivate(host string) bool {
	// First check if it's already an IP
	ip := net.ParseIP(host)
	if ip != nil {
		return isPrivateOrReservedIP(ip)
	}

	// It's a hostname - resolve it
	// We check ALL IPs that the hostname resolves to
	ips, err := net.LookupIP(host)
	if err != nil {
		// If we can't resolve, we should be conservative and reject
		// This prevents attacks using unresolvable hosts that might resolve differently later
		return true // Treat as private to be safe
	}

	// If any resolved IP is private, reject the hostname
	for _, resolvedIP := range ips {
		if isPrivateOrReservedIP(resolvedIP) {
			return true
		}
	}

	return false
}

// invalidateUserSessions increments the SessionVersion for a user, effectively invalidating
// all existing sessions. The session middleware checks SessionVersion on each request.
func (c *Controller) invalidateUserSessions(userID int) error {
	result := c.dbc.DB.Exec("UPDATE users SET session_version = session_version + 1 WHERE id = ?", userID)
	return result.Error
}

// ServeUpdateLastFMAPIKey shows the form to update Last.fm API key and secret
func (c *Controller) ServeUpdateLastFMAPIKey(r *http.Request) *Response {
	apiKey := c.dbc.GetSetting("lastfm_api_key", "")
	apiSecret := c.dbc.GetSetting("lastfm_api_secret", "")

	data := &templateData{
		CurrentLastFMAPIKey:    apiKey,
		CurrentLastFMAPISecret: apiSecret,
	}

	return &Response{
		template: "update_lastfm_api_key.tmpl",
		data:     data,
	}
}

// ServeUpdateLastFMAPIKeyDo handles saving the Last.fm API key and secret
func (c *Controller) ServeUpdateLastFMAPIKeyDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// CRITICAL: Re-verify acting user is still admin before updating API keys
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during Last.fm API key update by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	apiKey := strings.TrimSpace(r.FormValue("api_key"))
	apiSecret := strings.TrimSpace(r.FormValue("secret"))

	if err := c.dbc.SetSetting("lastfm_api_key", apiKey); err != nil {
		log.Printf("[ERROR] Failed to save lastfm_api_key: %v", err)
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"could not save Last.fm API key"},
		}
	}

	if err := c.dbc.SetSetting("lastfm_api_secret", apiSecret); err != nil {
		log.Printf("[ERROR] Failed to save lastfm_api_secret: %v", err)
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"could not save Last.fm API secret"},
		}
	}

	status := "updated"
	if apiKey == "" && apiSecret == "" {
		status = "cleared"
	}

	log.Printf("[AUDIT] Last.fm API keys %s by %s from %s", status, sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))

	return &Response{
		redirect: "/admin/home",
		flashN:   []string{fmt.Sprintf("Last.fm API keys %s successfully", status)},
	}
}

// Tidal OAuth Account Handlers

func (c *Controller) ServeTidalAccounts(r *http.Request) *Response {
	accounts, err := c.dbc.GetTidalAccounts()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get tidal accounts: %v", err)}
	}

	// Auto-detect subscription tier for existing accounts that don't have it yet
	for _, a := range accounts {
		if a.QualityTier == 0 || a.SubscriptionType == "" {
			if a.AccessToken != "" && a.UserID != "" {
				subType, highestQuality, tier, subErr := tidal.FetchAccountSubscription(r.Context(), a.UserID, a.AccessToken)
				if subErr == nil {
					_ = c.dbc.UpdateTidalAccountSubscription(a.ID, subType, highestQuality, tier)
					a.SubscriptionType = subType
					a.HighestQuality = highestQuality
					a.QualityTier = tier
				}
			}
		}
	}

	data := &templateData{}
	data.TidalAccounts = accounts

	c.pendingOAuthMu.Lock()
	data.PendingOAuth = c.pendingOAuth
	c.pendingOAuthMu.Unlock()

	return &Response{
		template: "tidal_accounts.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeTidalStartOAuthDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	dcr, err := tidal.RequestDeviceAuthorization(r.Context())
	if err != nil {
		log.Printf("[TIDAL] Failed to start OAuth device auth: %v", err)
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("failed to request Tidal login link: %v", err)}}
	}

	c.pendingOAuthMu.Lock()
	c.pendingOAuth = dcr
	c.pendingOAuthMu.Unlock()

	log.Printf("[TIDAL] Generated device authorization link: %s (code: %s)", dcr.VerificationURIComplete, dcr.UserCode)
	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{"authorization link created! Open link.tidal.com to approve."}}
}

func (c *Controller) ServeTidalCancelOAuthDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	c.pendingOAuthMu.Lock()
	c.pendingOAuth = nil
	c.pendingOAuthMu.Unlock()

	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{"OAuth login cancelled."}}
}

func (c *Controller) ServeTidalPollOAuthJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	c.pendingOAuthMu.Lock()
	pending := c.pendingOAuth
	c.pendingOAuthMu.Unlock()

	if pending == nil {
		json.NewEncoder(w).Encode(map[string]any{"status": "no_pending"})
		return
	}

	tok, err := tidal.PollForDeviceTokens(r.Context(), pending.DeviceCode)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": err.Error()})
		return
	}

	if tok == nil {
		json.NewEncoder(w).Encode(map[string]any{"status": "pending"})
		return
	}

	// Approved!
	userID := tok.GetUserIDStr()
	username := strings.TrimSpace(r.URL.Query().Get("username"))
	if username == "" {
		username = "Tidal (" + userID + ")"
	}
	expiresAt := time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)

	// Fetch subscription info
	subType, highestQuality, tier, subErr := tidal.FetchAccountSubscription(r.Context(), userID, tok.AccessToken)
	if subErr != nil {
		log.Printf("[TIDAL] Warning: could not determine subscription for %s: %v", userID, subErr)
	}

	_, err = c.dbc.AddTidalAccountWithQuality(userID, username, tok.RefreshToken, tok.AccessToken, subType, highestQuality, tier, expiresAt)
	if err != nil {
		log.Printf("[TIDAL] Error saving linked account to DB: %v", err)
		json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": "error saving account to database"})
		return
	}

	c.pendingOAuthMu.Lock()
	c.pendingOAuth = nil
	c.pendingOAuthMu.Unlock()

	log.Printf("[AUDIT] Tidal OAuth account linked successfully: %s (User ID: %s, Plan: %s, Tier: %d)", username, userID, subType, tier)
	json.NewEncoder(w).Encode(map[string]any{
		"status":            "success",
		"user_id":           userID,
		"username":          username,
		"subscription_type": subType,
		"quality_tier":      tier,
	})
}

func (c *Controller) ServeTidalAddManualDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	refreshToken := strings.TrimSpace(r.FormValue("refresh_token"))
	username := strings.TrimSpace(r.FormValue("username"))

	if refreshToken == "" {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"refresh token is required"}}
	}

	// Verify token immediately against Tidal
	tok, err := tidal.RefreshAccessToken(r.Context(), refreshToken)
	if err != nil {
		log.Printf("[TIDAL] Manual refresh token verification failed: %v", err)
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("token verification failed: %v", err)}}
	}

	userID := tok.GetUserIDStr()
	if username == "" {
		username = "Tidal (" + userID + ")"
	}
	expiresAt := time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)

	// Fetch subscription info
	subType, highestQuality, tier, subErr := tidal.FetchAccountSubscription(r.Context(), userID, tok.AccessToken)
	if subErr != nil {
		log.Printf("[TIDAL] Warning: could not determine subscription for %s: %v", userID, subErr)
	}

	if _, err := c.dbc.AddTidalAccountWithQuality(userID, username, refreshToken, tok.AccessToken, subType, highestQuality, tier, expiresAt); err != nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("could not save account: %v", err)}}
	}

	log.Printf("[AUDIT] Manual Tidal account added: %s (User ID: %s, Plan: %s, Tier: %d)", username, userID, subType, tier)
	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{fmt.Sprintf("Tidal account %s verified and added! (Plan: %s)", username, subType)}}
}

func (c *Controller) ServeTidalDeleteAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid account ID"}}
	}

	if err := c.dbc.DeleteTidalAccount(idInt); err != nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("delete account: %v", err)}}
	}

	log.Printf("[AUDIT] Tidal account deleted (ID: %d)", idInt)
	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{"Tidal account removed"}}
}

func (c *Controller) ServeTidalToggleAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid account ID"}}
	}

	active := r.FormValue("active") == "true"
	if err := c.dbc.ToggleTidalAccount(idInt, active); err != nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("update account: %v", err)}}
	}

	log.Printf("[AUDIT] Tidal account (ID: %d) set active=%v", idInt, active)
	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{"account status updated"}}
}

func (c *Controller) ServeTidalRefreshAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"invalid account ID"}}
	}

	accounts, err := c.dbc.GetTidalAccounts()
	if err != nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"failed to read accounts"}}
	}

	var target *db.TidalAccount
	for _, a := range accounts {
		if a.ID == idInt {
			target = a
			break
		}
	}

	if target == nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{"account not found"}}
	}

	tok, err := tidal.RefreshAccessToken(r.Context(), target.RefreshToken)
	if err != nil {
		log.Printf("[TIDAL] Failed to refresh token for account %d (%s): %v", target.ID, target.Username, err)
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("refresh failed: %v", err)}}
	}

	expiresAt := time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
	if err := c.dbc.UpdateTidalAccountTokens(target.ID, tok.AccessToken, expiresAt); err != nil {
		return &Response{redirect: "/admin/tidal_accounts", flashW: []string{fmt.Sprintf("database update failed: %v", err)}}
	}

	// Also detect and update subscription info
	subType, highestQuality, tier, subErr := tidal.FetchAccountSubscription(r.Context(), target.UserID, tok.AccessToken)
	if subErr == nil {
		_ = c.dbc.UpdateTidalAccountSubscription(target.ID, subType, highestQuality, tier)
	}

	log.Printf("[AUDIT] Tidal account %d (%s) refreshed. Valid until: %s (Plan: %s, Tier: %d)", target.ID, target.Username, expiresAt.Format(time.RFC3339), subType, tier)
	return &Response{redirect: "/admin/tidal_accounts", flashN: []string{fmt.Sprintf("token refreshed successfully! (valid for %d hours, Plan: %s)", tok.ExpiresIn/3600, subType)}}
}

type ProviderViewModel struct {
	ID              string
	Name            string
	Badge           string
	TypeDescription string
	MetadataEnabled bool
	AudioEnabled    bool
	AccountsCount   int
	Details         []string
}

func (c *Controller) ServeProviders(r *http.Request) *Response {
	tidalAccounts, _ := c.dbc.GetTidalAccounts()
	activeTidal := 0
	tidalDetails := []string{}
	for _, acc := range tidalAccounts {
		status := "inactive"
		if acc.IsActive {
			status = "active"
			activeTidal++
		}
		tidalDetails = append(tidalDetails, fmt.Sprintf("Account #%d: %s (%s)", acc.ID, acc.Username, status))
	}
	if len(tidalDetails) == 0 {
		tidalDetails = append(tidalDetails, "No accounts linked. Add one in 'Tidal Accounts'.")
	}

	var providerList []ProviderViewModel

	// Helper to check registry
	isMeta := func(id string) bool {
		if c.providers != nil {
			return c.providers.IsMetadataEnabled(id)
		}
		return true
	}
	isAudio := func(id string) bool {
		if c.providers != nil {
			return c.providers.IsAudioEnabled(id)
		}
		return true
	}

	// 1. Tidal
	providerList = append(providerList, ProviderViewModel{
		ID:              "td",
		Name:            "Tidal",
		Badge:           "HiFi / Master Lossless FLAC",
		TypeDescription: "Primary lossless provider with round-robin OAuth2 rotation",
		MetadataEnabled: isMeta("td"),
		AudioEnabled:    isAudio("td"),
		AccountsCount:   activeTidal,
		Details:         tidalDetails,
	})

	// 2. YouTube Music
	providerList = append(providerList, ProviderViewModel{
		ID:              "yt",
		Name:            "YouTube Music",
		Badge:           "Opus 160 kbps (No DRM)",
		TypeDescription: "Universal fallback provider & official Topic studio catalog",
		MetadataEnabled: isMeta("yt"),
		AudioEnabled:    isAudio("yt"),
		AccountsCount:   1,
		Details: []string{
			"Engine: yt-dlp (Python resident worker in RAM & CLI fallback)",
			"Catalog: InnerTube API (music.youtube.com official songs)",
			"Stream URL Caching: 1-hour in-memory TTL",
		},
	})

	// 3. Spotify
	providerList = append(providerList, ProviderViewModel{
		ID:              "sp",
		Name:            "Spotify",
		Badge:           "Metadata & ISRC Resolver",
		TypeDescription: "Global metadata bridge with multi-provider fallback",
		MetadataEnabled: isMeta("sp"),
		AudioEnabled:    isAudio("sp"),
		AccountsCount:   1,
		Details: []string{
			"Web Player API: Anonymous TOTP Client",
			"Fallback: Tidal FLAC -> YouTube Music Opus",
		},
	})

	// 4. Qobuz
	qobuzAccounts, _ := c.dbc.GetQobuzAccounts()
	activeQobuz := 0
	qobuzDetails := []string{}
	for _, acc := range qobuzAccounts {
		status := "inactive"
		if acc.IsActive {
			status = "active"
			activeQobuz++
		}
		qobuzDetails = append(qobuzDetails, fmt.Sprintf("Account #%d: %s [%s] (%s)", acc.ID, acc.Username, acc.HighestQuality, status))
	}
	if len(qobuzDetails) == 0 {
		qobuzDetails = append(qobuzDetails, "No accounts linked. Add via user_auth_token in 'Qobuz Accounts'.")
	} else {
		qobuzDetails = append(qobuzDetails, "Rotation: Round-Robin across active Studio/Sublime Hi-Res accounts")
	}
	qobuzDetails = append(qobuzDetails, "Direct Studio Master FLAC Delivery (up to 24-bit / 192kHz)")

	providerList = append(providerList, ProviderViewModel{
		ID:              "qb",
		Name:            "Qobuz",
		Badge:           "Hi-Res 24-bit FLAC",
		TypeDescription: "Official subscriber accounts via user_auth_token with round-robin rotation & Studio Hi-Res FLAC streaming",
		MetadataEnabled: isMeta("qb"),
		AudioEnabled:    isAudio("qb"),
		AccountsCount:   activeQobuz,
		Details:         qobuzDetails,
	})

	// 5. Deezer
	deezerAccounts, _ := c.dbc.GetDeezerAccounts()
	activeDeezer := 0
	deezerDetails := []string{}
	for _, acc := range deezerAccounts {
		status := "inactive"
		if acc.IsActive {
			status = "active"
			activeDeezer++
		}
		deezerDetails = append(deezerDetails, fmt.Sprintf("Account #%d: %s [%s] (%s)", acc.ID, acc.Username, acc.HighestQuality, status))
	}
	if len(deezerDetails) == 0 {
		deezerDetails = append(deezerDetails, "No accounts linked. Add via ARL in 'Deezer Accounts'.")
	} else {
		deezerDetails = append(deezerDetails, "Rotation: Round-Robin across active HiFi/Premium ARL accounts")
	}
	deezerDetails = append(deezerDetails, "Fallback Mirror: https://dzr.tabs-vs-spaces.wtf")

	providerList = append(providerList, ProviderViewModel{
		ID:              "dz",
		Name:            "Deezer",
		Badge:           "16-bit 44.1kHz FLAC",
		TypeDescription: "Official subscriber accounts via ARL with round-robin rotation & CD lossless FLAC streaming",
		MetadataEnabled: isMeta("dz"),
		AudioEnabled:    isAudio("dz"),
		AccountsCount:   activeDeezer,
		Details:         deezerDetails,
	})

	// 6. Amazon Music
	providerList = append(providerList, ProviderViewModel{
		ID:              "am",
		Name:            "Amazon Music",
		Badge:           "Ultra HD FLAC",
		TypeDescription: "High-definition audio mirror provider",
		MetadataEnabled: isMeta("am"),
		AudioEnabled:    isAudio("am"),
		AccountsCount:   1,
		Details: []string{
			"Mirror: https://amz.geeked.wtf",
		},
	})

	// 7. SoundCloud
	providerList = append(providerList, ProviderViewModel{
		ID:              "sc",
		Name:            "SoundCloud",
		Badge:           "HQ Audio / MP3 & Opus",
		TypeDescription: "Independent artist platform, DJ sets, unreleased tracks & remixes",
		MetadataEnabled: isMeta("sc"),
		AudioEnabled:    isAudio("sc"),
		AccountsCount:   1,
		Details: []string{
			"API: v2 Web Client with automatic client_id rotation",
			"Catalog: Millions of exclusive tracks, remixes, podcasts & sets",
			"Formats: Progressive MP3 (128 kbps) & HLS Opus",
		},
	})

	data := &templateData{
		Providers: providerList,
	}

	return &Response{
		template: "providers.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeToggleProviderDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/providers", flashW: []string{"invalid security token"}}
	}

	providerID := strings.TrimSpace(r.FormValue("provider_id"))
	sector := strings.TrimSpace(r.FormValue("sector")) // "metadata" or "audio"
	enabledStr := strings.TrimSpace(r.FormValue("enabled"))

	if providerID == "" || (sector != "metadata" && sector != "audio") {
		return &Response{redirect: "/admin/providers", flashW: []string{"invalid provider or sector"}}
	}

	enabled := enabledStr == "true" || enabledStr == "1"

	key := fmt.Sprintf("provider.%s.%s", providerID, sector)
	val := "false"
	if enabled {
		val = "true"
	}

	if err := c.dbc.SetSetting(key, val); err != nil {
		return &Response{redirect: "/admin/providers", flashW: []string{fmt.Sprintf("save error: %v", err)}}
	}

	if c.providers != nil {
		if sector == "metadata" {
			c.providers.SetMetadataEnabled(providerID, enabled)
		} else {
			c.providers.SetAudioEnabled(providerID, enabled)
		}
	}

	actionStr := "disabled"
	if enabled {
		actionStr = "enabled"
	}

	log.Printf("[AUDIT] Provider %s %s toggled to %s", providerID, sector, actionStr)
	return &Response{
		redirect: "/admin/providers",
		flashN:   []string{fmt.Sprintf("Provider %s: %s successfully %s!", strings.ToUpper(providerID), sector, actionStr)},
	}
}

func (c *Controller) ServeYTMusicAccounts(r *http.Request) *Response {
	accounts, err := c.dbc.GetYTMusicAccounts()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get ytmusic accounts: %v", err)}
	}

	data := &templateData{}
	data.YTMusicAccounts = accounts

	return &Response{
		template: "ytmusic_accounts.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeYTMusicAddAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"invalid security token"}}
	}

	name := strings.TrimSpace(r.FormValue("name"))
	cookieData := strings.TrimSpace(r.FormValue("cookie_data"))

	if name == "" {
		name = "YouTube Music Account"
	}
	if cookieData == "" {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"cookie data is required"}}
	}

	netscape, header, count, err := ytmusic.ParseCookies(cookieData)
	if err != nil {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{fmt.Sprintf("could not parse cookies: %v", err)}}
	}

	acc, err := c.dbc.AddYTMusicAccount(name, cookieData, netscape, header, false)
	if err != nil {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{fmt.Sprintf("could not save account: %v", err)}}
	}

	// Sync active account to provider
	if c.providers != nil {
		if p, err := c.providers.Get("yt"); err == nil && p != nil {
			if ytp, ok := p.(*ytmusic.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveYTMusicAccounts()
				ytp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] YouTube Music account added: %s (ID: %d, parsed %d cookies)", name, acc.ID, count)
	return &Response{
		redirect: "/admin/ytmusic_accounts",
		flashN:   []string{fmt.Sprintf("YouTube Music account %q added! (%d cookies parsed, fallback to free ready)", name, count)},
	}
}

func (c *Controller) ServeYTMusicToggleAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"invalid account ID"}}
	}

	active := r.FormValue("active") == "true"
	if err := c.dbc.ToggleYTMusicAccount(idInt, active); err != nil {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{fmt.Sprintf("update account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("yt"); err == nil && p != nil {
			if ytp, ok := p.(*ytmusic.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveYTMusicAccounts()
				ytp.SyncAccounts(activeAccs)
			}
		}
	}

	return &Response{redirect: "/admin/ytmusic_accounts", flashN: []string{"account status updated"}}
}

func (c *Controller) ServeYTMusicDeleteAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{"invalid account ID"}}
	}

	if err := c.dbc.DeleteYTMusicAccount(idInt); err != nil {
		return &Response{redirect: "/admin/ytmusic_accounts", flashW: []string{fmt.Sprintf("delete account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("yt"); err == nil && p != nil {
			if ytp, ok := p.(*ytmusic.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveYTMusicAccounts()
				ytp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] YouTube Music account deleted (ID: %d)", idInt)
	return &Response{redirect: "/admin/ytmusic_accounts", flashN: []string{"YouTube Music account removed"}}
}

func (c *Controller) ServeDeezerAccounts(r *http.Request) *Response {
	accounts, err := c.dbc.GetDeezerAccounts()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get deezer accounts: %v", err)}
	}

	data := &templateData{}
	data.DeezerAccounts = accounts

	return &Response{
		template: "deezer_accounts.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeDeezerAddAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid security token"}}
	}

	arl := strings.TrimSpace(r.FormValue("arl"))
	arl = strings.TrimPrefix(arl, "arl=")
	arl = strings.Trim(arl, "\";' ")
	arl = strings.TrimSpace(arl)
	if arl == "" {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"ARL token is required"}}
	}

	info, err := deezer.ValidateARL(r.Context(), arl)
	if err != nil {
		log.Printf("[AUDIT] Failed Deezer ARL validation: %v", err)
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{fmt.Sprintf("Deezer ARL validation failed: %v", err)}}
	}

	qualityLabel := info.HighestQuality
	if info.QualityTier >= db.QualityTierLossless {
		qualityLabel = "HiFi Lossless FLAC"
	} else if info.QualityTier >= db.QualityTierHigh {
		qualityLabel = "Premium 320k MP3"
	}

	// Check if account already exists to prevent duplicate rows
	existingAccounts, _ := c.dbc.GetDeezerAccounts()
	var existing *db.DeezerAccount
	for _, a := range existingAccounts {
		if a.ARL == arl || (a.UserID != "" && a.UserID == info.UserID) {
			existing = a
			break
		}
	}

	if existing != nil {
		existing.ARL = arl
		existing.UserID = info.UserID
		existing.Username = info.Username
		existing.SubscriptionType = info.SubscriptionType
		existing.HighestQuality = info.HighestQuality
		existing.QualityTier = info.QualityTier
		existing.IsActive = true
		existing.LastError = ""
		existing.LastValidatedAt = time.Now()
		_ = c.dbc.UpdateDeezerAccount(existing)

		// Sync active accounts to provider and reset cached session
		if c.providers != nil {
			if p, err := c.providers.Get("dz"); err == nil && p != nil {
				if dzp, ok := p.(*deezer.Provider); ok {
					dzp.Client().Pool().ResetAccountSession(existing.ID)
					activeAccs, _ := c.dbc.GetActiveDeezerAccounts()
					dzp.SyncAccounts(activeAccs)
				}
			}
		}

		log.Printf("[AUDIT] Deezer account updated: %s (User ID: %s, Plan: %s, Max: %s, ID: %d)", info.Username, info.UserID, info.SubscriptionType, qualityLabel, existing.ID)
		return &Response{
			redirect: "/admin/deezer_accounts",
			flashN:   []string{fmt.Sprintf("Deezer account %q (User ID: %s) updated and reactivated! (Quality: %s)", info.Username, info.UserID, qualityLabel)},
		}
	}

	acc, err := c.dbc.AddDeezerAccount(arl, info.UserID, info.Username, info.SubscriptionType, info.HighestQuality, info.QualityTier)
	if err != nil {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{fmt.Sprintf("could not save account: %v", err)}}
	}

	// Sync active accounts to provider
	if c.providers != nil {
		if p, err := c.providers.Get("dz"); err == nil && p != nil {
			if dzp, ok := p.(*deezer.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveDeezerAccounts()
				dzp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] Deezer account added: %s (User ID: %s, Plan: %s, Max: %s, ID: %d)", info.Username, info.UserID, info.SubscriptionType, qualityLabel, acc.ID)
	return &Response{
		redirect: "/admin/deezer_accounts",
		flashN:   []string{fmt.Sprintf("Deezer account %q added! (User ID: %s, Quality: %s)", info.Username, info.UserID, qualityLabel)},
	}
}

func (c *Controller) ServeDeezerToggleAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid account ID"}}
	}

	active := r.FormValue("active") == "true"
	if err := c.dbc.ToggleDeezerAccount(idInt, active); err != nil {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{fmt.Sprintf("update account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("dz"); err == nil && p != nil {
			if dzp, ok := p.(*deezer.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveDeezerAccounts()
				dzp.SyncAccounts(activeAccs)
			}
		}
	}

	return &Response{redirect: "/admin/deezer_accounts", flashN: []string{"account status updated"}}
}

func (c *Controller) ServeDeezerDeleteAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid account ID"}}
	}

	if err := c.dbc.DeleteDeezerAccount(idInt); err != nil {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{fmt.Sprintf("delete account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("dz"); err == nil && p != nil {
			if dzp, ok := p.(*deezer.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveDeezerAccounts()
				dzp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] Deezer account deleted (ID: %d)", idInt)
	return &Response{redirect: "/admin/deezer_accounts", flashN: []string{"Deezer account removed"}}
}

func (c *Controller) ServeDeezerRefreshAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"invalid account ID"}}
	}

	acc, err := c.dbc.GetDeezerAccountByID(idInt)
	if err != nil || acc == nil {
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{"account not found"}}
	}

	info, valErr := deezer.ValidateARL(r.Context(), acc.ARL)
	if valErr != nil {
		_ = c.dbc.UpdateDeezerAccountError(idInt, valErr.Error())
		return &Response{redirect: "/admin/deezer_accounts", flashW: []string{fmt.Sprintf("re-validation failed: %v", valErr)}}
	}

	_ = c.dbc.UpdateDeezerAccountSubscription(idInt, info.SubscriptionType, info.HighestQuality, info.QualityTier, "")
	if acc.Username != info.Username || acc.UserID != info.UserID {
		acc.Username = info.Username
		acc.UserID = info.UserID
		_ = c.dbc.UpdateDeezerAccount(acc)
	}

	if c.providers != nil {
		if p, err := c.providers.Get("dz"); err == nil && p != nil {
			if dzp, ok := p.(*deezer.Provider); ok {
				dzp.Client().Pool().ResetAccountSession(idInt)
				activeAccs, _ := c.dbc.GetActiveDeezerAccounts()
				dzp.SyncAccounts(activeAccs)
			}
		}
	}

	return &Response{
		redirect: "/admin/deezer_accounts",
		flashN:   []string{fmt.Sprintf("Deezer account %q successfully re-validated! (Plan: %s, Max: %s)", info.Username, info.SubscriptionType, info.HighestQuality)},
	}
}

func (c *Controller) ServeQobuzAccounts(r *http.Request) *Response {
	accounts, err := c.dbc.GetQobuzAccounts()
	if err != nil {
		return &Response{code: 500, err: fmt.Sprintf("get qobuz accounts: %v", err)}
	}

	data := &templateData{}
	data.QobuzAccounts = accounts

	return &Response{
		template: "qobuz_accounts.tmpl",
		data:     data,
	}
}

func (c *Controller) ServeQobuzAddAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid security token"}}
	}

	token := strings.TrimSpace(r.FormValue("token"))
	token = strings.TrimPrefix(token, "token=")
	token = strings.TrimPrefix(token, "user_auth_token=")
	token = strings.Trim(token, "\";' ")
	token = strings.TrimSpace(token)
	if token == "" {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"user_auth_token is required"}}
	}

	appID := strings.TrimSpace(r.FormValue("app_id"))
	appSecret := strings.TrimSpace(r.FormValue("app_secret"))

	if appID == "" {
		appID = c.dbc.GetSetting("qobuz.app_id", "")
	}
	if appSecret == "" {
		appSecret = c.dbc.GetSetting("qobuz.app_secret", "")
	}

	if appID == "" || appSecret == "" {
		if c.providers != nil {
			if p, err := c.providers.Get("qb"); err == nil && p != nil {
				if qbp, ok := p.(*qobuz.Provider); ok {
					if appID == "" {
						appID = qbp.Client().AppID()
					}
					if appSecret == "" {
						appSecret = qbp.Client().AppSecret()
					}
				}
			}
		}
	}

	info, err := qobuz.ValidateToken(r.Context(), appID, appSecret, token)
	if err != nil {
		log.Printf("[AUDIT] Failed Qobuz token validation: %v", err)
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{fmt.Sprintf("Qobuz token validation failed: %v", err)}}
	}

	// Persist and apply dynamic App ID / App Secret if explicitly provided in this form
	customAppID := strings.TrimSpace(r.FormValue("app_id"))
	customAppSecret := strings.TrimSpace(r.FormValue("app_secret"))
	if customAppID != "" {
		_ = c.dbc.SetSetting("qobuz.app_id", customAppID)
	}
	if customAppSecret != "" {
		_ = c.dbc.SetSetting("qobuz.app_secret", customAppSecret)
	}
	if customAppID != "" || customAppSecret != "" {
		if c.providers != nil {
			if p, err := c.providers.Get("qb"); err == nil && p != nil {
				if qbp, ok := p.(*qobuz.Provider); ok {
					qbp.SetCredentials(customAppID, customAppSecret)
				}
			}
		}
	}

	qualityLabel := info.HighestQuality
	if info.QualityTier >= 90 {
		qualityLabel = "Studio Hi-Res 24-bit FLAC"
	} else if info.QualityTier >= db.QualityTierLossless {
		qualityLabel = "HiFi Lossless 16-bit FLAC"
	} else if info.QualityTier >= db.QualityTierHigh {
		qualityLabel = "Premium 320k MP3"
	}

	// Check if account already exists to prevent duplicate rows
	existingAccounts, _ := c.dbc.GetQobuzAccounts()
	var existing *db.QobuzAccount
	for _, a := range existingAccounts {
		if a.UserAuthToken == token || (a.UserID != "" && a.UserID == info.UserID) {
			existing = a
			break
		}
	}

	if existing != nil {
		existing.UserAuthToken = token
		existing.UserID = info.UserID
		existing.Username = info.Username
		existing.SubscriptionType = info.SubscriptionType
		existing.HighestQuality = info.HighestQuality
		existing.QualityTier = info.QualityTier
		existing.IsActive = true
		existing.LastError = ""
		existing.LastValidatedAt = time.Now()
		_ = c.dbc.UpdateQobuzAccount(existing)

		// Sync active accounts to provider
		if c.providers != nil {
			if p, err := c.providers.Get("qb"); err == nil && p != nil {
				if qbp, ok := p.(*qobuz.Provider); ok {
					activeAccs, _ := c.dbc.GetActiveQobuzAccounts()
					qbp.SyncAccounts(activeAccs)
				}
			}
		}

		log.Printf("[AUDIT] Qobuz account updated: %s (User ID: %s, Plan: %s, Max: %s, ID: %d)", info.Username, info.UserID, info.SubscriptionType, qualityLabel, existing.ID)
		return &Response{
			redirect: "/admin/qobuz_accounts",
			flashN:   []string{fmt.Sprintf("Qobuz account %q (User ID: %s) updated and reactivated! (Quality: %s)", info.Username, info.UserID, qualityLabel)},
		}
	}

	acc, err := c.dbc.AddQobuzAccount(token, info.UserID, info.Username, info.SubscriptionType, info.HighestQuality, info.QualityTier)
	if err != nil {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{fmt.Sprintf("could not save account: %v", err)}}
	}

	// Sync active accounts to provider
	if c.providers != nil {
		if p, err := c.providers.Get("qb"); err == nil && p != nil {
			if qbp, ok := p.(*qobuz.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveQobuzAccounts()
				qbp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] Qobuz account added: %s (User ID: %s, Plan: %s, Max: %s, ID: %d)", info.Username, info.UserID, info.SubscriptionType, qualityLabel, acc.ID)
	return &Response{
		redirect: "/admin/qobuz_accounts",
		flashN:   []string{fmt.Sprintf("Qobuz account %q added! (User ID: %s, Quality: %s)", info.Username, info.UserID, qualityLabel)},
	}
}

func (c *Controller) ServeQobuzToggleAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid account ID"}}
	}

	active := r.FormValue("active") == "true"
	if err := c.dbc.ToggleQobuzAccount(idInt, active); err != nil {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{fmt.Sprintf("update account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("qb"); err == nil && p != nil {
			if qbp, ok := p.(*qobuz.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveQobuzAccounts()
				qbp.SyncAccounts(activeAccs)
			}
		}
	}

	return &Response{redirect: "/admin/qobuz_accounts", flashN: []string{"account status updated"}}
}

func (c *Controller) ServeQobuzDeleteAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid account ID"}}
	}

	if err := c.dbc.DeleteQobuzAccount(idInt); err != nil {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{fmt.Sprintf("delete account: %v", err)}}
	}

	if c.providers != nil {
		if p, err := c.providers.Get("qb"); err == nil && p != nil {
			if qbp, ok := p.(*qobuz.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveQobuzAccounts()
				qbp.SyncAccounts(activeAccs)
			}
		}
	}

	log.Printf("[AUDIT] Qobuz account deleted (ID: %d)", idInt)
	return &Response{redirect: "/admin/qobuz_accounts", flashN: []string{"Qobuz account removed"}}
}

func (c *Controller) ServeQobuzRefreshAccountDo(r *http.Request) *Response {
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid security token"}}
	}

	idStr := r.FormValue("id")
	idInt, err := strconv.Atoi(idStr)
	if err != nil || idInt <= 0 {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"invalid account ID"}}
	}

	acc, err := c.dbc.GetQobuzAccountByID(idInt)
	if err != nil || acc == nil {
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{"account not found"}}
	}

	appID := ""
	appSecret := ""
	if c.providers != nil {
		if p, err := c.providers.Get("qb"); err == nil && p != nil {
			if qbp, ok := p.(*qobuz.Provider); ok {
				appID = qbp.Client().AppID()
				appSecret = qbp.Client().AppSecret()
			}
		}
	}

	info, valErr := qobuz.ValidateToken(r.Context(), appID, appSecret, acc.UserAuthToken)
	if valErr != nil {
		_ = c.dbc.UpdateQobuzAccountError(idInt, valErr.Error())
		return &Response{redirect: "/admin/qobuz_accounts", flashW: []string{fmt.Sprintf("re-validation failed: %v", valErr)}}
	}

	_ = c.dbc.UpdateQobuzAccountSubscription(idInt, info.SubscriptionType, info.HighestQuality, info.QualityTier, "")
	if acc.Username != info.Username || acc.UserID != info.UserID {
		acc.Username = info.Username
		acc.UserID = info.UserID
		_ = c.dbc.UpdateQobuzAccount(acc)
	}

	if c.providers != nil {
		if p, err := c.providers.Get("qb"); err == nil && p != nil {
			if qbp, ok := p.(*qobuz.Provider); ok {
				activeAccs, _ := c.dbc.GetActiveQobuzAccounts()
				qbp.SyncAccounts(activeAccs)
			}
		}
	}

	return &Response{
		redirect: "/admin/qobuz_accounts",
		flashN:   []string{fmt.Sprintf("Qobuz account %q successfully re-validated! (Plan: %s, Max: %s)", info.Username, info.SubscriptionType, info.HighestQuality)},
	}
}


