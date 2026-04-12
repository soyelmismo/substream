package ctrladmin

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/sessions"
	"github.com/sentriz/gormstore"

	"go.senan.xyz/gonic"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/handlerutil"
	"go.senan.xyz/gonic/server/ctrladmin/adminui"
	"go.senan.xyz/gonic/tidalproxy"
)

type CtxKey int

const (
	CtxUser CtxKey = iota
	CtxSession
)

type Controller struct {
	*http.ServeMux

	dbc              *db.DB
	sessDB           *gormstore.Store
	proxy            tidalproxy.TidalProxy
	resolveProxyPath ProxyPathResolver
	rateLimiter      *RateLimiter
}

// RateLimiter provides simple IP-based rate limiting for login attempts
type RateLimiter struct {
	mu       sync.RWMutex
	attempts map[string]*loginAttempts
}

type loginAttempts struct {
	count   int
	lastTry time.Time
	locked  bool
}

const (
	maxLoginAttempts           = 5
	lockoutDuration            = 15 * time.Minute
	maxRateLimiterEntries      = 10000 // Prevent unbounded memory growth
	rateLimiterCleanupInterval = 5 * time.Minute
)

func NewRateLimiter() *RateLimiter {
	rl := &RateLimiter{
		attempts: make(map[string]*loginAttempts),
	}
	// Start background cleanup goroutine
	go rl.backgroundCleanup()
	return rl
}

// backgroundCleanup periodically removes old entries to prevent memory growth
func (rl *RateLimiter) backgroundCleanup() {
	ticker := time.NewTicker(rateLimiterCleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		rl.cleanup()
	}
}

// cleanup removes expired entries (called periodically, not during CheckLimit)
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for ip, attempts := range rl.attempts {
		if time.Since(attempts.lastTry) > lockoutDuration {
			delete(rl.attempts, ip)
		}
	}
}

func (rl *RateLimiter) CheckLimit(ip string) (bool, string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Note: Cleanup is now done periodically in background, not here
	// to prevent DoS via expensive map iteration on every request

	attempts, exists := rl.attempts[ip]
	if !exists {
		// Enforce max entries limit to prevent memory exhaustion
		if len(rl.attempts) >= maxRateLimiterEntries {
			// Remove oldest entry when at capacity (simple LRU eviction)
			var oldestIP string
			var oldestTime time.Time
			for ip, att := range rl.attempts {
				if oldestTime.IsZero() || att.lastTry.Before(oldestTime) {
					oldestIP = ip
					oldestTime = att.lastTry
				}
			}
			if oldestIP != "" {
				delete(rl.attempts, oldestIP)
			}
		}
		rl.attempts[ip] = &loginAttempts{count: 1, lastTry: time.Now()}
		return true, ""
	}

	// Check if locked
	if attempts.locked {
		if time.Since(attempts.lastTry) < lockoutDuration {
			remaining := lockoutDuration - time.Since(attempts.lastTry)
			return false, fmt.Sprintf("account locked. try again in %d minutes", int(remaining.Minutes())+1)
		}
		// Unlock after duration
		attempts.locked = false
		attempts.count = 1
		attempts.lastTry = time.Now()
		return true, ""
	}

	// Increment attempt
	attempts.count++
	attempts.lastTry = time.Now()

	if attempts.count >= maxLoginAttempts {
		attempts.locked = true
		return false, "too many failed attempts. account locked for 15 minutes"
	}

	return true, ""
}

func (rl *RateLimiter) Reset(ip string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.attempts, ip)
}

type ProxyPathResolver func(in string) string

func New(dbc *db.DB, sessDB *gormstore.Store, proxy tidalproxy.TidalProxy, resolveProxyPath ProxyPathResolver) (*Controller, error) {
	c := Controller{
		ServeMux: http.NewServeMux(),

		dbc:              dbc,
		sessDB:           sessDB,
		proxy:            proxy,
		resolveProxyPath: resolveProxyPath,
		rateLimiter:      NewRateLimiter(),
	}

	resp := respHandler(adminui.TemplatesFS, resolveProxyPath)

	// Security headers middleware for all admin routes
	securityChain := withSecurityHeaders()
	baseChain := handlerutil.Chain(
		withSession(sessDB),
		securityChain,
	)
	userChain := handlerutil.Chain(
		baseChain,
		withUserSession(dbc, resolveProxyPath),
	)
	adminChain := handlerutil.Chain(
		userChain,
		withAdminSession(resolveProxyPath),
	)

	c.Handle("/static/", http.FileServer(http.FS(adminui.StaticFS)))

	// public routes (creates session)
	c.Handle("/login", baseChain(resp(c.ServeLogin)))
	c.Handle("/login_do", baseChain(respRaw(c.ServeLoginDo)))

	// user routes (if session is valid)
	// logout routes - confirmation page and action
	c.Handle("/logout", userChain(resp(c.ServeLogout)))
	c.Handle("/logout_do", userChain(respRaw(c.ServeLogoutDo)))
	c.Handle("/change_username", userChain(resp(c.ServeChangeUsername)))
	c.Handle("/change_username_do", userChain(resp(c.ServeChangeUsernameDo)))
	c.Handle("/change_password", userChain(resp(c.ServeChangePassword)))
	c.Handle("/change_password_do", userChain(resp(c.ServeChangePasswordDo)))
	c.Handle("/change_avatar", userChain(resp(c.ServeChangeAvatar)))
	c.Handle("/change_avatar_do", userChain(resp(c.ServeChangeAvatarDo)))
	c.Handle("/delete_avatar_do", userChain(resp(c.ServeDeleteAvatarDo)))
	c.Handle("/delete_user", userChain(resp(c.ServeDeleteUser)))
	c.Handle("/delete_user_do", userChain(resp(c.ServeDeleteUserDo)))
	c.Handle("/unlink_lastfm_do", userChain(resp(c.ServeUnlinkLastFMDo)))
	c.Handle("/link_lastfm_start", userChain(resp(c.ServeLinkLastFMStart)))
	c.Handle("/link_lastfm_callback", userChain(resp(c.ServeLinkLastFMCallback)))
	c.Handle("/link_listenbrainz_do", userChain(resp(c.ServeLinkListenBrainzDo)))
	c.Handle("/unlink_listenbrainz_do", userChain(resp(c.ServeUnlinkListenBrainzDo)))
	c.Handle("/profile", userChain(resp(c.ServeProfile)))

	// admin routes (if session is valid, and is admin)
	c.Handle("/home", adminChain(resp(c.ServeHome)))
	c.Handle("/create_user", adminChain(resp(c.ServeCreateUser)))
	c.Handle("/create_user_do", adminChain(resp(c.ServeCreateUserDo)))

	// admin settings
	c.Handle("/settings", adminChain(resp(c.ServeSettings)))
	c.Handle("/settings_do", adminChain(resp(c.ServeSettingsDo)))

	// admin last.fm api key configuration
	c.Handle("/update_lastfm_api_key", adminChain(resp(c.ServeUpdateLastFMAPIKey)))
	c.Handle("/update_lastfm_api_key_do", adminChain(resp(c.ServeUpdateLastFMAPIKeyDo)))

	// admin proxies
	c.Handle("/proxies", adminChain(resp(c.ServeProxies)))
	c.Handle("/add_proxy_do", adminChain(resp(c.ServeAddProxyDo)))
	c.Handle("/delete_proxy_do", adminChain(resp(c.ServeDeleteProxyDo)))

	// admin cache
	c.Handle("/clear_cache_do", adminChain(resp(c.ServeClearCacheDo)))

	// Unused endpoints removed

	// Root path redirects to profile for authenticated users, login otherwise
	c.Handle("/", baseChain(resp(c.ServeRoot)))

	return &c, nil
}

// isHTTPS returns true if the request is using HTTPS
func isHTTPS(r *http.Request) bool {
	// Check if the connection is TLS
	if r.TLS != nil {
		return true
	}
	// Check X-Forwarded-Proto header (common with reverse proxies)
	if r.Header.Get("X-Forwarded-Proto") == "https" {
		return true
	}
	return false
}

func withSession(sessDB *gormstore.Store) handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session, err := sessDB.Get(r, gonic.Name)
			if err != nil {
				http.Error(w, fmt.Sprintf("error getting session: %s", err), 500)
				return
			}
			// Debug: log session info
			isNew := session.IsNew
			log.Printf("[DEBUG] withSession: loaded session ID=%s, isNew=%v, path=%s", session.ID, isNew, r.URL.Path)
			log.Printf("[DEBUG] withSession: loaded session values count=%d", len(session.Values))
			for k, v := range session.Values {
				log.Printf("[DEBUG] withSession: loaded session.Values[%v] = %v (type=%T)", k, v, v)
			}
			// Harden session cookie settings
			session.Options.HttpOnly = true
			session.Options.SameSite = http.SameSiteStrictMode
			session.Options.Path = "/" // Make cookie available site-wide
			// Set Secure flag when using HTTPS to prevent cookie theft via MITM
			if isHTTPS(r) {
				session.Options.Secure = true
			}
			// Generate CSRF token if not present (needed for all forms including login)
			if session.Values["csrf_token"] == nil {
				session.Values["csrf_token"] = generateCSRFToken()
				log.Printf("[DEBUG] withSession: generated new CSRF token, saving session ID=%s", session.ID)
				if err := session.Save(r, w); err != nil {
					log.Printf("error saving session: %v\n", err)
				}
			}
			withSession := context.WithValue(r.Context(), CtxSession, session)
			next.ServeHTTP(w, r.WithContext(withSession))
		})
	}
}

func withUserSession(dbc *db.DB, resolvePath func(string) string) handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// session exists at this point
			session := r.Context().Value(CtxSession).(*sessions.Session)
			userID, ok := session.Values["user"].(int)
			if !ok {
				// Debug: dump all session values
				log.Printf("[DEBUG] withUserSession: no user in session, path=%s, session ID=%s", r.URL.Path, session.ID)
				log.Printf("[DEBUG] Session values count: %d", len(session.Values))
				for k, v := range session.Values {
					log.Printf("[DEBUG] Session key=%v (type=%T)", k, v)
				}
				sessAddFlashW(session, []string{"you are not authenticated"})
				sessLogSave(session, w, r)
				http.Redirect(w, r, resolvePath("/admin/login"), http.StatusSeeOther)
				return
			}

			// Check session version to detect invalidated sessions (e.g., after password change)
			sessionVersion, _ := session.Values["session_version"].(int)

			// take username from sesion and add the user row to the context
			user := dbc.GetUserByID(userID)
			if user == nil {
				// the username in the client's session no longer relates to a
				// user in the database (maybe the user was deleted)
				sessAddFlashW(session, []string{"invalid_user: your account may have been deleted"})
				session.Options.MaxAge = -1
				sessLogSave(session, w, r)
				http.Redirect(w, r, resolvePath("/admin/login"), http.StatusSeeOther)
				return
			}

			// Verify session version matches current user session version
			// If they don't match, the user's password was changed or session was invalidated
			if sessionVersion != user.SessionVersion {
				sessAddFlashW(session, []string{"your session has been invalidated. please log in again."})
				session.Options.MaxAge = -1
				sessLogSave(session, w, r)
				http.Redirect(w, r, resolvePath("/admin/login"), http.StatusSeeOther)
				return
			}

			// CSRF token is already generated in withSession middleware
			withUser := context.WithValue(r.Context(), CtxUser, user)
			next.ServeHTTP(w, r.WithContext(withUser))
		})
	}
}

// generateCSRFToken creates a random CSRF token
func generateCSRFToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// validateCSRFToken validates CSRF token from request against session
func validateCSRFToken(r *http.Request) bool {
	session := r.Context().Value(CtxSession).(*sessions.Session)
	expectedToken, ok := session.Values["csrf_token"].(string)
	if !ok || expectedToken == "" {
		log.Printf("[SECURITY] CSRF validation failed: no token in session from %s", getClientIP(r))
		return false
	}

	// Check header first (for AJAX), then form value
	providedToken := r.Header.Get("X-CSRF-Token")
	if providedToken == "" {
		providedToken = r.FormValue("csrf_token")
	}

	if providedToken == "" {
		log.Printf("[SECURITY] CSRF validation failed: no token provided from %s", getClientIP(r))
		return false
	}

	if subtle.ConstantTimeCompare([]byte(expectedToken), []byte(providedToken)) != 1 {
		log.Printf("[SECURITY] CSRF validation failed: token mismatch from %s", getClientIP(r))
		return false
	}

	return true
}

// csrfToken returns the CSRF token for the current session (for templates)
func csrfToken(r *http.Request) string {
	session := r.Context().Value(CtxSession).(*sessions.Session)
	if token, ok := session.Values["csrf_token"].(string); ok {
		return token
	}
	return ""
}

func withAdminSession(resolveProxyPath ProxyPathResolver) handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// session and user exist at this point
			session := r.Context().Value(CtxSession).(*sessions.Session)
			user := r.Context().Value(CtxUser).(*db.User)
			if !user.IsAdmin {
				sessAddFlashW(session, []string{"you are not an admin"})
				sessLogSave(session, w, r)
				http.Redirect(w, r, resolveProxyPath("/admin/login"), http.StatusSeeOther)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// withSecurityHeaders adds security headers to prevent common attacks
func withSecurityHeaders() handlerutil.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Prevent clickjacking
			w.Header().Set("X-Frame-Options", "DENY")
			// Prevent MIME type sniffing
			w.Header().Set("X-Content-Type-Options", "nosniff")
			// XSS protection for older browsers
			w.Header().Set("X-XSS-Protection", "1; mode=block")
			// Referrer policy
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
			// Permissions policy
			w.Header().Set("Permissions-Policy", "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()")
			// Content Security Policy - restrictive but allows inline styles (needed for current UI)
			w.Header().Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' 'unsafe-inline'; "+ // unsafe-inline needed for potential inline scripts
					"style-src 'self' 'unsafe-inline'; "+
					"img-src 'self' data: blob:; "+
					"font-src 'self'; "+
					"connect-src 'self'; "+
					"frame-ancestors 'none'; "+
					"base-uri 'self'; "+
					"form-action 'self';")
			// Add HSTS header for HTTPS connections (tells browser to always use HTTPS)
			if isHTTPS(r) {
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload")
			}
			next.ServeHTTP(w, r)
		})
	}
}

type StatsData struct {
	Albums  int64
	Artists int64
	Tracks  int64
}

type Response struct {
	// code is 200
	template string
	data     *templateData
	// code is 303
	redirect string
	flashN   []string // normal
	flashW   []string // warning
	// code is >= 400
	code int
	err  string
}

type (
	handlerAdmin func(r *http.Request) *Response
)

func respHandler(templateFS embed.FS, resolvePath func(string) string) func(next handlerAdmin) http.Handler {
	tmpl := template.Must(template.
		New("layout").
		Funcs(template.FuncMap(sprig.FuncMap())).
		Funcs(funcMap()).
		Funcs(template.FuncMap{"path": resolvePath}).
		ParseFS(templateFS, "*.tmpl", "**/*.tmpl"),
	)
	buffPool := sync.Pool{New: func() any { return new(bytes.Buffer) }}

	return func(next handlerAdmin) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := next(r)
			session, ok := r.Context().Value(CtxSession).(*sessions.Session)
			if ok {
				sessAddFlashN(session, resp.flashN)
				sessAddFlashW(session, resp.flashW)
				if err := session.Save(r, w); err != nil {
					http.Error(w, fmt.Sprintf("error saving session: %v", err), 500)
					return
				}
			}
			if resp.redirect != "" {
				http.Redirect(w, r, resolvePath(resp.redirect), http.StatusSeeOther)
				return
			}
			if resp.err != "" {
				http.Error(w, resp.err, resp.code)
				return
			}
			if resp.template == "" {
				http.Error(w, "useless handler return", 500)
				return
			}

			if resp.data == nil {
				resp.data = &templateData{}
			}
			resp.data.Version = gonic.Version
			resp.data.CSRFToken = csrfToken(r)
			if session != nil {
				resp.data.Flashes = session.Flashes()
				if err := session.Save(r, w); err != nil {
					http.Error(w, fmt.Sprintf("error saving session: %v", err), 500)
					return
				}
			}
			if user, ok := r.Context().Value(CtxUser).(*db.User); ok {
				resp.data.User = user
			}

			buff := buffPool.Get().(*bytes.Buffer)
			defer buffPool.Put(buff)
			buff.Reset()

			if err := tmpl.ExecuteTemplate(buff, resp.template, resp.data); err != nil {
				http.Error(w, fmt.Sprintf("executing template: %v", err), 500)
				return
			}

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if resp.code != 0 {
				w.WriteHeader(resp.code)
			}
			if _, err := buff.WriteTo(w); err != nil {
				log.Printf("error writing to response buffer: %v\n", err)
			}
		})
	}
}

func respRaw(h http.HandlerFunc) http.Handler {
	return h // stub
}

type templateData struct {
	// common
	Flashes   []any
	User      *db.User
	Version   string
	CSRFToken string

	// home
	Stats       StatsData
	RequestRoot string

	AllUsers []*db.User

	CurrentLastFMAPIKey    string
	CurrentLastFMAPISecret string
	DefaultListenBrainzURL string
	SelectedUser           *db.User

	// avatar
	Avatar []byte

	// proxies
	Proxies     []*db.ProxyInstance
	MirrorStats string

	// settings
	AutoRegister bool
	ProxyStreams bool

	// cache
	CacheStats tidalproxy.CacheStats
}

func funcMap() template.FuncMap {
	return template.FuncMap{
		"str": func(in any) string {
			v, _ := json.Marshal(in)
			return string(v)
		},
		"noCache": func(in string) string {
			parsed, _ := url.Parse(in)
			params := parsed.Query()
			params.Set("v", gonic.Version)
			parsed.RawQuery = params.Encode()
			return parsed.String()
		},
		"date": func(in time.Time) string {
			return strings.ToLower(in.Format("Jan 02, 2006"))
		},
		"dateHuman": humanize.Time,
		"base64":    base64.StdEncoding.EncodeToString,
		// urlquery escapes a string for safe use in URL query parameters
		"urlquery": url.QueryEscape,
		// htmlEscape escapes a string for safe use in HTML content
		"htmlEscape": html.EscapeString,
	}
}

//  utilities

type FlashType string

const (
	FlashNormal  = FlashType("normal")
	FlashWarning = FlashType("warning")
)

type Flash struct {
	Message string
	Type    FlashType
}

// gob registrations are next to each other, in case there's more added later)
//
//nolint:gochecknoinits // for now I think it's nice that our types and their
func init() {
	gob.Register(&Flash{})
}

func sessAddFlashN(s *sessions.Session, messages []string) {
	sessAddFlash(s, messages, FlashNormal)
}

func sessAddFlashW(s *sessions.Session, messages []string) {
	sessAddFlash(s, messages, FlashWarning)
}

func sessAddFlash(s *sessions.Session, messages []string, flashT FlashType) {
	if len(messages) == 0 {
		return
	}
	for i, message := range messages {
		if i > 6 {
			break
		}
		s.AddFlash(Flash{
			Message: message,
			Type:    flashT,
		})
	}
}

func sessLogSave(s *sessions.Session, w http.ResponseWriter, r *http.Request) {
	if err := s.Save(r, w); err != nil {
		log.Printf("error saving session: %v\n", err)
	}
}

// validation functions are now in handlers_raw.go for better security practices
