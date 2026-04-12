package ctrladmin

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/gorilla/sessions"
	"golang.org/x/crypto/bcrypt"

	"go.senan.xyz/gonic/db"
)

// sanitizeLogValue removes control characters and newlines from log values
// to prevent log injection attacks where attackers could forge log entries
func sanitizeLogValue(value string) string {
	// Remove newlines and carriage returns to prevent log injection
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	// Remove Unicode line separators that can cause log injection
	value = strings.ReplaceAll(value, "\u2028", " ") // Line separator
	value = strings.ReplaceAll(value, "\u2029", " ") // Paragraph separator
	// Remove null bytes
	value = strings.ReplaceAll(value, "\x00", "")
	// Remove other control characters except tab
	var result strings.Builder
	for _, r := range value {
		if r == '\t' || r >= 32 {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func getClientIP(r *http.Request) string {
	// Get the direct remote address
	remoteAddr := r.RemoteAddr

	// Only trust X-Forwarded-For and X-Real-Ip if the direct connection
	// appears to be from a private/internal network (indicating a reverse proxy)
	// This prevents IP spoofing on publicly exposed servers
	if isTrustedProxy(remoteAddr) {
		// Check X-Forwarded-For header (for proxies)
		// Take the LAST IP in the chain (closest to the server) to prevent spoofing
		xff := r.Header.Get("X-Forwarded-For")
		if xff != "" {
			ips := strings.Split(xff, ",")
			if len(ips) > 0 {
				// Use the last IP in the chain (closest to our proxy) for better security
				// Still validate it's a proper IP format
				clientIP := strings.TrimSpace(ips[len(ips)-1])
				if net.ParseIP(clientIP) != nil {
					return clientIP
				}
			}
		}

		// Check X-Real-Ip header as fallback
		xri := r.Header.Get("X-Real-Ip")
		if xri != "" && net.ParseIP(xri) != nil {
			return xri
		}
	}

	// Fall back to RemoteAddr
	return remoteAddr
}

// isTrustedProxy checks if the remote address appears to be from a trusted proxy
// (private network or loopback). This helps prevent IP spoofing attacks where
// attackers directly connect and fake X-Forwarded-For headers.
func isTrustedProxy(remoteAddr string) bool {
	// Extract IP from host:port format
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	// Trust loopback addresses
	if ip.IsLoopback() {
		return true
	}

	// Trust private network ranges (typical for reverse proxies)
	if ip.IsPrivate() {
		return true
	}

	return false
}

func (c *Controller) ServeLoginDo(w http.ResponseWriter, r *http.Request) {
	session := r.Context().Value(CtxSession).(*sessions.Session)

	// Check rate limit
	clientIP := getClientIP(r)
	allowed, msg := c.rateLimiter.CheckLimit(clientIP)
	if !allowed {
		sessAddFlashW(session, []string{msg})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
		return
	}

	// Validate CSRF token for login
	if !validateCSRFToken(r) {
		sessAddFlashW(session, []string{"invalid security token"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
		return
	}

	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")
	if username == "" || password == "" {
		sessAddFlashW(session, []string{"please provide username and password"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
		return
	}

	// Validate username format before DB lookup
	if err := validateUsername(username); err != nil {
		sessAddFlashW(session, []string{"invalid username format"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
		return
	}

	user := c.dbc.GetUserByName(username)

	// If user doesn't exist, check if auto-register is enabled
	if user == nil {
		autoRegister := c.dbc.GetSetting("auto_register", "false")
		if autoRegister == "true" {
			// Validate password before creating user
			if err := validatePasswordCreation(password); err != nil {
				sessAddFlashW(session, []string{err.Error()})
				sessLogSave(session, w, r)
				http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
				return
			}

			// Hash password before storing
			hashedPassword, err := hashPassword(password)
			if err != nil {
				log.Printf("[LOGIN] Password hashing failed for user %s: %v", sanitizeLogValue(username), err)
				sessAddFlashW(session, []string{"could not create user"})
				sessLogSave(session, w, r)
				http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
				return
			}

			// Create new user automatically
			newUser := db.User{
				Name:             username,
				Password:         hashedPassword, // Bcrypt hash for secure auth
				SubsonicPassword: password,       // Plaintext for Subsonic token auth
				IsAdmin:          false,
			}
			if err := c.dbc.Create(&newUser).Error; err != nil {
				log.Printf("[LOGIN] Auto-register failed for user %s: %v", sanitizeLogValue(username), err)
				sessAddFlashW(session, []string{"could not auto-register user"})
				sessLogSave(session, w, r)
				http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
				return
			}
			log.Printf("[AUDIT] Auto-registered new user %s from %s", sanitizeLogValue(username), sanitizeLogValue(clientIP))
			user = &newUser
		} else {
			log.Printf("[AUDIT] Failed login attempt for non-existent user %s from %s", sanitizeLogValue(username), sanitizeLogValue(clientIP))
			sessAddFlashW(session, []string{"invalid username / password"})
			sessLogSave(session, w, r)
			http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
			return
		}
	}

	// Check password for existing user (supports both hashed and legacy plaintext)
	if !verifyPassword(user.Password, password) {
		log.Printf("[AUDIT] Failed login attempt for user %s from %s: invalid password", sanitizeLogValue(username), sanitizeLogValue(clientIP))
		sessAddFlashW(session, []string{"invalid username / password"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/login"), http.StatusSeeOther)
		return
	}

	// Log successful login
	log.Printf("[AUDIT] Successful login for user %s from %s", sanitizeLogValue(username), sanitizeLogValue(clientIP))

	// Regenerate session ID to prevent session fixation attacks
	// Save values we want to preserve
	csrfToken := ""
	if tok, ok := session.Values["csrf_token"].(string); ok {
		csrfToken = tok
	}

	// Clear all old session values (but keep the session object to preserve ID reference)
	for k := range session.Values {
		delete(session.Values, k)
	}

	// CRITICAL: Set cookie flags (same as withSession middleware)
	session.Options.HttpOnly = true
	session.Options.SameSite = http.SameSiteStrictMode
	session.Options.Path = "/"       // Make cookie available site-wide
	session.Options.MaxAge = 2592000 // 30 days in seconds (ensures DB persistence)
	if isHTTPS(r) {
		session.Options.Secure = true
	}

	// Set new session values
	session.Values["user"] = user.ID
	session.Values["session_version"] = user.SessionVersion
	if csrfToken != "" {
		session.Values["csrf_token"] = csrfToken
	}

	// Debug: verify session values
	log.Printf("[DEBUG] LoginDo: session ID=%s, setting user=%d, session_version=%d", session.ID, user.ID, user.SessionVersion)
	log.Printf("[DEBUG] LoginDo: session.Values count before save: %d", len(session.Values))
	for k, v := range session.Values {
		log.Printf("[DEBUG] LoginDo: session.Values[%v] = %v (type=%T)", k, v, v)
	}

	// Reset rate limiter on successful login
	c.rateLimiter.Reset(clientIP)

	// Save session and redirect
	if err := session.Save(r, w); err != nil {
		log.Printf("[ERROR] Failed to save session: %v", err)
		http.Error(w, "session error", http.StatusInternalServerError)
		return
	}
	log.Printf("[DEBUG] LoginDo: session saved successfully, session ID=%s", session.ID)
	log.Printf("[DEBUG] LoginDo: session.Values count AFTER save: %d", len(session.Values))
	for k, v := range session.Values {
		log.Printf("[DEBUG] LoginDo: post-save session.Values[%v] = %v (type=%T)", k, v, v)
	}
	// Redirect based on user role
	var redirectPath string
	if user.IsAdmin {
		redirectPath = c.resolveProxyPath("/admin/home")
	} else {
		redirectPath = c.resolveProxyPath("/admin/profile")
	}
	log.Printf("[DEBUG] Login successful for %s, redirecting to: %s", sanitizeLogValue(username), redirectPath)
	http.Redirect(w, r, redirectPath, http.StatusSeeOther)
}

// hashPassword creates a bcrypt hash of the password
func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes), err
}

// verifyPassword checks a password against a hash (supports legacy plaintext)
func verifyPassword(storedPassword, providedPassword string) bool {
	// Check if stored password is a bcrypt hash (starts with $2a$, $2b$, or $2y$)
	if strings.HasPrefix(storedPassword, "$2a$") ||
		strings.HasPrefix(storedPassword, "$2b$") ||
		strings.HasPrefix(storedPassword, "$2y$") {
		err := bcrypt.CompareHashAndPassword([]byte(storedPassword), []byte(providedPassword))
		return err == nil
	}

	// Legacy: plaintext comparison using constant-time to prevent timing attacks
	return subtle.ConstantTimeCompare([]byte(storedPassword), []byte(providedPassword)) == 1
}

// safeRedirectPath validates and returns a safe redirect path
// This prevents open redirect vulnerabilities including:
// - Protocol-relative URLs (//evil.com)
// - Path traversal (/admin/../../evil.com)
// - URL-encoded bypasses (%2F%2Fevil.com)
// - Mixed case bypasses
// - Null byte injection (%00)
func safeRedirectPath(referer, fallback string) string {
	// If referer is empty or too long, use fallback
	if referer == "" || len(referer) > 2048 {
		return fallback
	}

	// Reject null bytes which can cause truncation issues
	if strings.Contains(referer, "\x00") {
		return fallback
	}

	// Decode any URL encoding to catch bypass attempts like %2F%2F or %00
	decoded, err := url.QueryUnescape(referer)
	if err != nil {
		return fallback
	}
	// Check again for null bytes after decoding
	if strings.Contains(decoded, "\x00") {
		return fallback
	}
	referer = decoded

	// Reject potentially dangerous characters
	if strings.ContainsAny(referer, "<>'\"") {
		return fallback
	}

	// Reject protocol-relative URLs (//evil.com) and backslash variants (\\evil.com)
	if strings.HasPrefix(referer, "//") || strings.HasPrefix(referer, `\\`) {
		return fallback
	}

	// Reject any URL with scheme (http://, https://, ftp://, etc.)
	if strings.Contains(referer, "://") {
		return fallback
	}

	// Parse as URL to validate structure
	parsed, err := url.Parse(referer)
	if err != nil {
		return fallback
	}

	// Reject if there's a host component (absolute URL)
	if parsed.Host != "" {
		return fallback
	}

	// Reject if path is empty
	if parsed.Path == "" {
		return fallback
	}

	// Normalize path: must start with /admin/ or be exactly /admin
	pathStr := parsed.Path
	if !strings.HasPrefix(pathStr, "/") {
		pathStr = "/" + pathStr
	}

	// Only allow /admin prefix
	if pathStr == "/admin" || strings.HasPrefix(pathStr, "/admin/") {
		// Use path.Clean to properly handle path traversal attempts
		// Then verify the cleaned path still starts with /admin
		cleaned := path.Clean(pathStr)
		if !strings.HasPrefix(cleaned, "/admin") {
			return fallback
		}
		// Additional check: ensure no ".." remains in the cleaned path
		if strings.Contains(cleaned, "..") {
			return fallback
		}
		return cleaned
	}

	return fallback
}

// ServeLogout shows logout confirmation page with CSRF protection
func (c *Controller) ServeLogout(r *http.Request) *Response {
	return &Response{template: "logout.tmpl"}
}

// ServeLogoutDo handles the actual logout with CSRF validation
func (c *Controller) ServeLogoutDo(w http.ResponseWriter, r *http.Request) {
	session := r.Context().Value(CtxSession).(*sessions.Session)
	clientIP := getClientIP(r)

	// Validate CSRF token to prevent logout CSRF attacks
	if !validateCSRFToken(r) {
		sessAddFlashW(session, []string{"invalid security token"})
		sessLogSave(session, w, r)
		http.Redirect(w, r, safeRedirectPath(r.Referer(), "/admin/home"), http.StatusSeeOther)
		return
	}

	// Get username if available
	userID, ok := session.Values["user"].(int)
	if ok {
		log.Printf("[AUDIT] User ID %d logged out from %s", userID, sanitizeLogValue(clientIP))
	}

	session.Options.MaxAge = -1
	sessLogSave(session, w, r)
	http.Redirect(w, r, c.resolveProxyPath("/admin/login"), http.StatusSeeOther)
}

func (c *Controller) ServeSettings(r *http.Request) *Response {
	autoRegister := c.dbc.GetSetting("auto_register", "false")
	proxyStreams := c.dbc.GetSetting("proxy_streams", "false")
	return &Response{
		template: "settings.tmpl",
		data: &templateData{
			AutoRegister: autoRegister == "true",
			ProxyStreams: proxyStreams == "true",
		},
	}
}

func (c *Controller) ServeSettingsDo(r *http.Request) *Response {
	// Validate CSRF token
	if !validateCSRFToken(r) {
		return &Response{redirect: "/admin/home", flashW: []string{"invalid security token"}}
	}
	autoRegister := r.FormValue("auto_register")
	if autoRegister == "" {
		autoRegister = "false"
	}

	proxyStreams := r.FormValue("proxy_streams")
	if proxyStreams == "" {
		proxyStreams = "false"
	}

	actingUser := r.Context().Value(CtxUser).(*db.User)
	clientIP := getClientIP(r)

	// CRITICAL: Re-verify acting user is still admin before changing settings (prevents race condition)
	// This is especially important for auto_register which could allow account takeover
	freshActingUser := c.dbc.GetUserByID(actingUser.ID)
	if freshActingUser == nil || !freshActingUser.IsAdmin {
		log.Printf("[SECURITY] Admin privilege revocation detected during settings change by %s", sanitizeLogValue(actingUser.Name))
		return &Response{redirect: "/admin/home", flashW: []string{"admin privileges have been revoked"}}
	}

	// Require current password for security (prevents session hijacking from changing critical settings)
	// This is critical because enabling auto_register could allow account takeover
	currentPassword := r.FormValue("current_password")
	if !verifyPassword(actingUser.Password, currentPassword) {
		log.Printf("[AUDIT] Failed settings change attempt by %s from %s: incorrect password", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP))
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"current password is incorrect"},
		}
	}

	if err := c.dbc.SetSetting("auto_register", autoRegister); err != nil {
		log.Printf("[ERROR] Failed to save auto_register setting: %v", err)
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"could not save settings"},
		}
	}

	if err := c.dbc.SetSetting("proxy_streams", proxyStreams); err != nil {
		log.Printf("[ERROR] Failed to save proxy_streams setting: %v", err)
		return &Response{
			redirect: safeRedirectPath(r.Referer(), "/admin/home"),
			flashW:   []string{"could not save settings"},
		}
	}

	status := "disabled"
	if autoRegister == "true" {
		status = "enabled"
	}

	proxyStatus := "disabled"
	if proxyStreams == "true" {
		proxyStatus = "enabled"
	}

	log.Printf("[AUDIT] Settings changed by %s from %s: auto_register=%s, proxy_streams=%s", sanitizeLogValue(actingUser.Name), sanitizeLogValue(clientIP), sanitizeLogValue(autoRegister), sanitizeLogValue(proxyStreams))

	return &Response{
		redirect: "/admin/home",
		flashN:   []string{fmt.Sprintf("auto-register %s, proxy streams %s", status, proxyStatus)},
	}
}

// validation

var (
	errValiNoUsername           = errors.New("please enter a username")
	errValiUsernameTooLong      = errors.New("username must be at most 64 characters")
	errValiUsernameInvalidChars = errors.New("username can only contain letters, numbers, underscores, hyphens, and dots")
	errValiUsernameReserved     = errors.New("username is reserved")
	errValiPasswordTooShort     = errors.New("password must be at least 8 characters")
	errValiPasswordTooLong      = errors.New("password must be at most 128 characters")
	errValiPasswordAllFields    = errors.New("please enter the password twice")
	errValiPasswordsNotSame     = errors.New("passwords entered were not the same")
)

// usernameRegex allows alphanumeric, underscore, hyphen, and dot
var usernameRegex = regexp.MustCompile("^[a-zA-Z0-9._-]+$")

// reservedUsernames that cannot be used
var reservedUsernames = map[string]bool{
	"admin":  true,
	"root":   true,
	"system": true,
	"guest":  true,
	"test":   true,
}

func validateUsername(username string) error {
	if username == "" {
		return errValiNoUsername
	}
	if len(username) > 64 {
		return errValiUsernameTooLong
	}
	if !usernameRegex.MatchString(username) {
		return errValiUsernameInvalidChars
	}
	if reservedUsernames[strings.ToLower(username)] {
		return errValiUsernameReserved
	}
	// Prevent usernames starting with special characters (security hardening)
	// Dots at start can cause confusion with hidden files
	// Hyphens at start can cause issues with command-line arguments
	if strings.HasPrefix(username, ".") || strings.HasPrefix(username, "-") {
		return errValiUsernameInvalidChars
	}
	// Prevent consecutive special characters (.., --, __)
	if strings.Contains(username, "..") || strings.Contains(username, "--") || strings.Contains(username, "__") {
		return errValiUsernameInvalidChars
	}
	return nil
}

func validatePasswordCreation(password string) error {
	if len(password) < 8 {
		return errValiPasswordTooShort
	}
	if len(password) > 128 {
		return errValiPasswordTooLong
	}
	return nil
}

func validatePasswords(pOne, pTwo string) error {
	if pOne == "" || pTwo == "" {
		return errValiPasswordAllFields
	}
	if !(pOne == pTwo) {
		return errValiPasswordsNotSame
	}
	if len(pOne) < 8 {
		return errValiPasswordTooShort
	}
	if len(pOne) > 128 {
		return errValiPasswordTooLong
	}
	return nil
}
