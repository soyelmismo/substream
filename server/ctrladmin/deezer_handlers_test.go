package ctrladmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/sessions"
	"github.com/sentriz/gormstore"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/provider/deezer"
)

func setupTestAdminController(t *testing.T) (*Controller, *db.DB, *provider.Registry) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	dbc, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	if err := dbc.Migrate(); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}

	sessDB := gormstore.New(dbc.DB, []byte("test-secret-key-1234"))
	ctrl, err := New(dbc, sessDB, nil, func(s string) string { return s })
	if err != nil {
		t.Fatalf("failed to create admin controller: %v", err)
	}

	reg := provider.NewRegistry()
	dzProv := deezer.New(deezer.ClientOptions{
		CacheDir: t.TempDir(),
		DB:       dbc,
	})
	reg.Register(dzProv)
	ctrl.SetProviders(reg)

	return ctrl, dbc, reg
}

func addMockSession(r *http.Request, csrfToken string) *http.Request {
	sess := &sessions.Session{
		Values: map[interface{}]interface{}{
			"csrf_token": csrfToken,
			"user":       1,
		},
	}
	ctx := context.WithValue(r.Context(), CtxSession, sess)
	return r.WithContext(ctx)
}

func TestServeDeezerAccountsEmpty(t *testing.T) {
	ctrl, _, _ := setupTestAdminController(t)
	r := httptest.NewRequest("GET", "/admin/deezer_accounts", nil)
	r = addMockSession(r, "test_token")

	resp := ctrl.ServeDeezerAccounts(r)
	if resp.template != "deezer_accounts.tmpl" {
		t.Errorf("expected template 'deezer_accounts.tmpl', got %q", resp.template)
	}
	td := resp.data
	if td == nil {
		t.Fatalf("expected non-nil resp.data")
	}
	if len(td.DeezerAccounts) != 0 {
		t.Errorf("expected 0 accounts, got %d", len(td.DeezerAccounts))
	}
}

func TestServeDeezerAddAccountDoCSRFProtection(t *testing.T) {
	ctrl, _, _ := setupTestAdminController(t)
	form := url.Values{"csrf_token": {"wrong_token"}, "arl": {"dummy"}}
	r := httptest.NewRequest("POST", "/admin/deezer_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, "correct_token")

	resp := ctrl.ServeDeezerAddAccountDo(r)
	if len(resp.flashW) == 0 || !strings.Contains(resp.flashW[0], "invalid security token") {
		t.Errorf("expected CSRF error, got: %v", resp.flashW)
	}
}

func TestServeDeezerAddAccountDoLiveAndLifecycle(t *testing.T) {
	dummyARL := "dummy_arl_test_token_0123456789abcdef0123456789abcdef0123456789abcdef"
	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		c, _ := r.Cookie("arl")
		if c == nil || c.Value != dummyARL {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "AUTH_REQUIRED",
				"results": map[string]interface{}{
					"USER": map[string]interface{}{
						"USER_ID": "0",
					},
				},
			})
			return
		}

		_, _ = w.Write([]byte(`{
			"results": {
				"USER": {
					"USER_ID": "6426929883",
					"BLOG_NAME": "CEO The Producer",
					"OPTIONS": {
						"license_token": "mock_license_token",
						"web_lossless": true,
						"web_hq": true,
						"expiration_date": "2099-12-31"
					}
				},
				"checkForm": "mock_check_form"
			}
		}`))
	}))
	t.Cleanup(mockSrv.Close)
	t.Cleanup(deezer.SetGatewayURLsForTest(mockSrv.URL+"/ajax/gw-light.php", ""))

	ctrl, dbc, reg := setupTestAdminController(t)
	csrf := "valid_csrf_123"

	// 1. Add account via ARL
	form := url.Values{"csrf_token": {csrf}, "arl": {dummyARL}}
	r := httptest.NewRequest("POST", "/admin/deezer_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, csrf)

	resp := ctrl.ServeDeezerAddAccountDo(r)
	if len(resp.flashW) > 0 {
		t.Fatalf("unexpected warning flash: %v", resp.flashW)
	}
	if len(resp.flashN) == 0 || !strings.Contains(resp.flashN[0], "CEO The Producer") {
		t.Fatalf("expected success notification with username, got: %v", resp.flashN)
	}

	// Verify account in DB
	accounts, err := dbc.GetDeezerAccounts()
	if err != nil || len(accounts) != 1 {
		t.Fatalf("expected 1 account in DB, got %d (err: %v)", len(accounts), err)
	}
	acc := accounts[0]
	if acc.Username != "CEO The Producer" {
		t.Errorf("expected username 'CEO The Producer', got %q", acc.Username)
	}
	if acc.QualityTier != 80 {
		t.Errorf("expected QualityTier 80, got %d", acc.QualityTier)
	}
	if !acc.IsActive {
		t.Errorf("expected account to be active")
	}

	// Verify provider registry synced account
	p, err := reg.Get("dz")
	if err != nil {
		t.Fatalf("get dz provider failed: %v", err)
	}
	dzp := p.(*deezer.Provider)
	if dzp.Client().Pool().Count() != 1 {
		t.Errorf("expected 1 account in provider pool, got %d", dzp.Client().Pool().Count())
	}

	// 1b. Attempt duplicate add with 'arl=' prefix and quotes
	formDup := url.Values{"csrf_token": {csrf}, "arl": {"arl=\"" + dummyARL + "\""}}
	rDup := httptest.NewRequest("POST", "/admin/deezer_add_account_do", strings.NewReader(formDup.Encode()))
	rDup.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rDup = addMockSession(rDup, csrf)
	respDup := ctrl.ServeDeezerAddAccountDo(rDup)
	if len(respDup.flashW) > 0 {
		t.Fatalf("unexpected warning on duplicate add: %v", respDup.flashW)
	}
	// Verify still exactly 1 account in DB (no duplication)
	accountsDup, _ := dbc.GetDeezerAccounts()
	if len(accountsDup) != 1 {
		t.Fatalf("expected still 1 account in DB after duplicate add, got %d", len(accountsDup))
	}

	// 2. Toggle account to inactive
	idStr := strconv.Itoa(acc.ID)
	toggleForm := url.Values{"csrf_token": {csrf}, "id": {idStr}, "active": {"false"}}
	rToggle := httptest.NewRequest("POST", "/admin/deezer_toggle_account_do", strings.NewReader(toggleForm.Encode()))
	rToggle.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rToggle = addMockSession(rToggle, csrf)

	respToggle := ctrl.ServeDeezerToggleAccountDo(rToggle)
	if len(respToggle.flashW) > 0 {
		t.Errorf("unexpected toggle warning: %v", respToggle.flashW)
	}

	updatedAcc, _ := dbc.GetDeezerAccountByID(acc.ID)
	if updatedAcc.IsActive {
		t.Errorf("expected account to be inactive after toggle")
	}
	if dzp.Client().Pool().Count() != 0 {
		t.Errorf("expected 0 active accounts in provider pool after toggle, got %d", dzp.Client().Pool().Count())
	}

	// 3. Toggle back to active
	toggleForm2 := url.Values{"csrf_token": {csrf}, "id": {idStr}, "active": {"true"}}
	rToggle2 := httptest.NewRequest("POST", "/admin/deezer_toggle_account_do", strings.NewReader(toggleForm2.Encode()))
	rToggle2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rToggle2 = addMockSession(rToggle2, csrf)
	ctrl.ServeDeezerToggleAccountDo(rToggle2)

	// 4. Refresh / Recheck account
	rRefresh := httptest.NewRequest("POST", "/admin/deezer_refresh_account_do", strings.NewReader(url.Values{"csrf_token": {csrf}, "id": {idStr}}.Encode()))
	rRefresh.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rRefresh = addMockSession(rRefresh, csrf)

	respRefresh := ctrl.ServeDeezerRefreshAccountDo(rRefresh)
	if len(respRefresh.flashW) > 0 {
		t.Errorf("unexpected refresh warning: %v", respRefresh.flashW)
	}
	if len(respRefresh.flashN) == 0 || !strings.Contains(respRefresh.flashN[0], "re-validated") {
		t.Errorf("expected success notification on re-validation, got: %v", respRefresh.flashN)
	}

	// 5. Delete account
	delForm := url.Values{"csrf_token": {csrf}, "id": {idStr}}
	rDel := httptest.NewRequest("POST", "/admin/deezer_delete_account_do", strings.NewReader(delForm.Encode()))
	rDel.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rDel = addMockSession(rDel, csrf)

	respDel := ctrl.ServeDeezerDeleteAccountDo(rDel)
	if len(respDel.flashW) > 0 {
		t.Errorf("unexpected delete warning: %v", respDel.flashW)
	}

	postDelAccs, _ := dbc.GetDeezerAccounts()
	if len(postDelAccs) != 0 {
		t.Errorf("expected 0 accounts after delete, got %d", len(postDelAccs))
	}
	if dzp.Client().Pool().Count() != 0 {
		t.Errorf("expected 0 active accounts in provider pool after delete, got %d", dzp.Client().Pool().Count())
	}
}

func TestServeDeezerAddAccountDoInvalidARL(t *testing.T) {
	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "AUTH_REQUIRED",
			"results": map[string]interface{}{
				"USER": map[string]interface{}{
					"USER_ID": "0",
				},
			},
		})
	}))
	t.Cleanup(mockSrv.Close)
	t.Cleanup(deezer.SetGatewayURLsForTest(mockSrv.URL+"/ajax/gw-light.php", ""))

	ctrl, dbc, _ := setupTestAdminController(t)
	csrf := "csrf_valid_invalid_test"

	form := url.Values{"csrf_token": {csrf}, "arl": {"invalid_or_expired_arl_string"}}
	r := httptest.NewRequest("POST", "/admin/deezer_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, csrf)

	resp := ctrl.ServeDeezerAddAccountDo(r)
	if len(resp.flashW) == 0 {
		t.Fatalf("expected warning flash for invalid ARL, got none")
	}
	if !strings.Contains(resp.flashW[0], "Deezer ARL validation failed") {
		t.Errorf("expected validation failure flash, got %q", resp.flashW[0])
	}

	// Verify no accounts were added to DB
	accs, _ := dbc.GetDeezerAccounts()
	if len(accs) != 0 {
		t.Errorf("expected 0 accounts in DB, got %d", len(accs))
	}
}

func TestServeDeezerAddAccountDoEmptyARL(t *testing.T) {
	ctrl, dbc, _ := setupTestAdminController(t)
	csrf := "csrf_valid_empty_test"

	form := url.Values{"csrf_token": {csrf}, "arl": {"   "}}
	r := httptest.NewRequest("POST", "/admin/deezer_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, csrf)

	resp := ctrl.ServeDeezerAddAccountDo(r)
	if len(resp.flashW) == 0 || !strings.Contains(resp.flashW[0], "ARL token is required") {
		t.Errorf("expected 'ARL token is required' warning, got %v", resp.flashW)
	}

	accs, _ := dbc.GetDeezerAccounts()
	if len(accs) != 0 {
		t.Errorf("expected 0 accounts in DB, got %d", len(accs))
	}
}

