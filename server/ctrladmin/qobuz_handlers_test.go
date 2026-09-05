package ctrladmin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sentriz/gormstore"
	"go.senan.xyz/gonic/db"
	"go.senan.xyz/gonic/provider"
	"go.senan.xyz/gonic/provider/qobuz"
)

func setupTestAdminControllerWithQobuz(t *testing.T) (*Controller, *db.DB, *provider.Registry) {
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
	qbProv := qobuz.New(qobuz.ClientOptions{
		DB: dbc,
	})
	reg.Register(qbProv)
	ctrl.SetProviders(reg)

	return ctrl, dbc, reg
}

func TestServeQobuzAccountsEmpty(t *testing.T) {
	ctrl, _, _ := setupTestAdminControllerWithQobuz(t)
	r := httptest.NewRequest("GET", "/admin/qobuz_accounts", nil)
	r = addMockSession(r, "test_token")

	resp := ctrl.ServeQobuzAccounts(r)
	if resp.template != "qobuz_accounts.tmpl" {
		t.Errorf("expected template 'qobuz_accounts.tmpl', got %q", resp.template)
	}
	td := resp.data
	if td == nil {
		t.Fatalf("expected non-nil resp.data")
	}
	if len(td.QobuzAccounts) != 0 {
		t.Errorf("expected 0 accounts, got %d", len(td.QobuzAccounts))
	}
}

func TestServeQobuzAddAccountDoCSRFProtection(t *testing.T) {
	ctrl, _, _ := setupTestAdminControllerWithQobuz(t)
	form := url.Values{"csrf_token": {"wrong_token"}, "token": {"dummy"}}
	r := httptest.NewRequest("POST", "/admin/qobuz_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, "correct_token")

	resp := ctrl.ServeQobuzAddAccountDo(r)
	if len(resp.flashW) == 0 || !strings.Contains(resp.flashW[0], "invalid security token") {
		t.Errorf("expected CSRF error, got: %v", resp.flashW)
	}
}

func TestServeQobuzAddAccountDoEmptyToken(t *testing.T) {
	ctrl, _, _ := setupTestAdminControllerWithQobuz(t)
	csrf := "valid_csrf_token"
	form := url.Values{"csrf_token": {csrf}, "token": {""}}
	r := httptest.NewRequest("POST", "/admin/qobuz_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, csrf)

	resp := ctrl.ServeQobuzAddAccountDo(r)
	if len(resp.flashW) == 0 || !strings.Contains(resp.flashW[0], "user_auth_token is required") {
		t.Errorf("expected error for empty token, got: %v", resp.flashW)
	}
}

func TestServeQobuzAddAccountDoLiveAndLifecycle(t *testing.T) {
	dummyToken := "test_user_auth_token_studio_123"
	mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		token := r.URL.Query().Get("user_auth_token")
		if token == "" {
			token = r.Header.Get("X-User-Auth-Token")
		}

		if token != dummyToken {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "error",
				"error":  "invalid_auth_token",
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":           14474460,
			"login":        "audiophile_tester",
			"display_name": "Audiophile Master",
			"email":        "audio@example.com",
			"credential": map[string]interface{}{
				"parameters": map[string]interface{}{
					"hires_streaming":    true,
					"lossless_streaming": true,
				},
			},
		})
	}))
	defer mockSrv.Close()

	cleanup := qobuz.SetAPIBaseURLForTest(mockSrv.URL)
	t.Cleanup(cleanup)

	ctrl, dbc, reg := setupTestAdminControllerWithQobuz(t)
	csrf := "csrf_123"

	// 1. Add valid account
	form := url.Values{
		"csrf_token": {csrf},
		"token":      {dummyToken},
	}
	r := httptest.NewRequest("POST", "/admin/qobuz_add_account_do", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = addMockSession(r, csrf)

	resp := ctrl.ServeQobuzAddAccountDo(r)
	if resp.redirect != "/admin/qobuz_accounts" {
		t.Errorf("expected redirect to /admin/qobuz_accounts, got %q", resp.redirect)
	}
	if len(resp.flashN) == 0 || !strings.Contains(resp.flashN[0], "added") {
		t.Errorf("expected success notification, got: %v (flashW: %v)", resp.flashN, resp.flashW)
	}

	// Verify in DB
	accounts, err := dbc.GetQobuzAccounts()
	if err != nil || len(accounts) != 1 {
		t.Fatalf("expected 1 account in DB, got %d (err: %v)", len(accounts), err)
	}
	acc := accounts[0]
	if acc.Username != "Audiophile Master" || acc.UserID != "14474460" || acc.SubscriptionType != "STUDIO" || acc.QualityTier < 90 {
		t.Errorf("unexpected account properties: %+v", acc)
	}

	// Verify Provider pool synchronization
	p, _ := reg.Get("qb")
	qbp := p.(*qobuz.Provider)
	if qbp.Client().Pool().Count() != 1 {
		t.Errorf("expected 1 account in provider pool, got %d", qbp.Client().Pool().Count())
	}

	// 2. Duplicate submission updates instead of creating duplicate
	formDup := url.Values{
		"csrf_token": {csrf},
		"token":      {dummyToken},
	}
	rDup := httptest.NewRequest("POST", "/admin/qobuz_add_account_do", strings.NewReader(formDup.Encode()))
	rDup.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rDup = addMockSession(rDup, csrf)

	respDup := ctrl.ServeQobuzAddAccountDo(rDup)
	if len(respDup.flashN) == 0 || !strings.Contains(respDup.flashN[0], "updated") {
		t.Errorf("expected updated notification, got: %v", respDup.flashN)
	}
	accounts, _ = dbc.GetQobuzAccounts()
	if len(accounts) != 1 {
		t.Fatalf("expected still 1 account in DB after re-add, got %d", len(accounts))
	}

	// 3. Toggle account (disable)
	idStr := strconv.Itoa(acc.ID)
	toggleForm := url.Values{"csrf_token": {csrf}, "id": {idStr}, "active": {"false"}}
	rToggle := httptest.NewRequest("POST", "/admin/qobuz_toggle_account_do", strings.NewReader(toggleForm.Encode()))
	rToggle.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rToggle = addMockSession(rToggle, csrf)

	ctrl.ServeQobuzToggleAccountDo(rToggle)
	updatedAcc, _ := dbc.GetQobuzAccountByID(acc.ID)
	if updatedAcc.IsActive {
		t.Errorf("expected account to be inactive")
	}
	if qbp.Client().Pool().Count() != 0 {
		t.Errorf("expected 0 active accounts in pool after disabling, got %d", qbp.Client().Pool().Count())
	}

	// 4. Toggle account (enable)
	toggleForm2 := url.Values{"csrf_token": {csrf}, "id": {idStr}, "active": {"true"}}
	rToggle2 := httptest.NewRequest("POST", "/admin/qobuz_toggle_account_do", strings.NewReader(toggleForm2.Encode()))
	rToggle2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rToggle2 = addMockSession(rToggle2, csrf)

	ctrl.ServeQobuzToggleAccountDo(rToggle2)
	updatedAcc2, _ := dbc.GetQobuzAccountByID(acc.ID)
	if !updatedAcc2.IsActive {
		t.Errorf("expected account to be active")
	}

	// 5. Recheck/Refresh account
	rRefresh := httptest.NewRequest("POST", "/admin/qobuz_refresh_account_do", strings.NewReader(url.Values{"csrf_token": {csrf}, "id": {idStr}}.Encode()))
	rRefresh.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rRefresh = addMockSession(rRefresh, csrf)

	respRefresh := ctrl.ServeQobuzRefreshAccountDo(rRefresh)
	if len(respRefresh.flashN) == 0 || !strings.Contains(respRefresh.flashN[0], "re-validated") {
		t.Errorf("expected re-validated flash, got: %v (flashW: %v)", respRefresh.flashN, respRefresh.flashW)
	}

	// 6. Delete account
	delForm := url.Values{"csrf_token": {csrf}, "id": {idStr}}
	rDel := httptest.NewRequest("POST", "/admin/qobuz_delete_account_do", strings.NewReader(delForm.Encode()))
	rDel.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rDel = addMockSession(rDel, csrf)

	respDel := ctrl.ServeQobuzDeleteAccountDo(rDel)
	if len(respDel.flashN) == 0 || !strings.Contains(respDel.flashN[0], "removed") {
		t.Errorf("expected removed flash, got: %v", respDel.flashN)
	}

	accountsAfterDel, _ := dbc.GetQobuzAccounts()
	if len(accountsAfterDel) != 0 {
		t.Errorf("expected 0 accounts in DB after deletion, got %d", len(accountsAfterDel))
	}
	if qbp.Client().Pool().Count() != 0 {
		t.Errorf("expected 0 accounts in pool after deletion, got %d", qbp.Client().Pool().Count())
	}
}
