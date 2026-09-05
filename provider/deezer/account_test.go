package deezer

import (
	"fmt"
	"net/http/cookiejar"
	"net/url"
	"testing"

	"go.senan.xyz/gonic/db"
)

func TestAccountPoolRotation(t *testing.T) {
	pool := NewAccountPool(nil)

	// Add 3 accounts: 2 HiFi (tier 80) and 1 Free (tier 10)
	accounts := []*db.DeezerAccount{
		{ID: 1, ARL: "arl1", Username: "User 1", QualityTier: 80, IsActive: true},
		{ID: 2, ARL: "arl2", Username: "User 2", QualityTier: 80, IsActive: true},
		{ID: 3, ARL: "arl3", Username: "User 3", QualityTier: 10, IsActive: true},
	}
	pool.SyncAccounts(accounts)

	if pool.Count() != 3 {
		t.Fatalf("expected 3 accounts in pool, got %d", pool.Count())
	}

	// When requesting Lossless (tier 80), only User 1 and User 2 should rotate
	seen := make(map[int]int)
	for i := 0; i < 10; i++ {
		sess, err := pool.GetNextAccount(db.QualityTierLossless)
		if err != nil {
			t.Fatalf("GetNextAccount failed: %v", err)
		}
		seen[sess.Account.ID]++
	}

	if seen[3] > 0 {
		t.Errorf("Free account (ID 3) was returned for Lossless stream request")
	}
	if seen[1] != 5 || seen[2] != 5 {
		t.Errorf("expected even round-robin rotation between ID 1 and 2 (got %v)", seen)
	}

	// Test error marking
	pool.MarkError(1, fmt.Errorf("temporary network timeout"))
	// ID 1 should still be active
	sess, _ := pool.GetNextAccount(db.QualityTierLossless)
	if sess == nil {
		t.Fatalf("expected active session")
	}

	// Mark permanent invalid ARL error
	pool.MarkError(1, fmt.Errorf("invalid ARL: session expired"))
	// Account 1 should now be deactivated
	if pool.Count() != 2 {
		t.Errorf("expected active count to decrease to 2, got %d", pool.Count())
	}

	for i := 0; i < 5; i++ {
		sess, err := pool.GetNextAccount(db.QualityTierLossless)
		if err != nil {
			t.Fatalf("GetNextAccount failed: %v", err)
		}
		if sess.Account.ID == 1 {
			t.Errorf("deactivated account ID 1 was returned from pool")
		}
	}

	// Verify HasLosslessAccount
	if !pool.HasLosslessAccount() {
		t.Errorf("expected pool to have lossless account (User 2)")
	}
	// Deactivate User 2
	pool.MarkError(2, fmt.Errorf("session expired"))
	if pool.HasLosslessAccount() {
		t.Errorf("expected pool to NOT have lossless account after User 2 deactivation")
	}
}

func TestResetAccountSession(t *testing.T) {
	acc := &db.DeezerAccount{ID: 1, ARL: "arl_test", Username: "Test"}
	sess := NewAccountSession(acc)
	sess.licenseToken = "tok123"
	sess.checkForm = "csrf456"

	sess.ResetSession()
	if sess.licenseToken != "" || sess.checkForm != "" {
		t.Errorf("expected tokens to be cleared after ResetSession")
	}
}

func TestSetARLCookie(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("create jar: %v", err)
	}

	// 1. Production Deezer HTTPS endpoint
	prodURL := "https://www.deezer.com/ajax/gw-light.php"
	setARLCookie(jar, prodURL, "prod_arl_token")

	uProd, _ := url.Parse(prodURL)
	cookiesProd := jar.Cookies(uProd)
	var foundProd bool
	for _, c := range cookiesProd {
		if c.Name == "arl" && c.Value == "prod_arl_token" {
			foundProd = true
			break
		}
	}
	if !foundProd {
		t.Errorf("expected arl cookie in jar for %s, got: %v", prodURL, cookiesProd)
	}

	// Also verify cookie is sent to media.deezer.com if domain is .deezer.com
	uMedia, _ := url.Parse("https://media.deezer.com/v1/get_url")
	cookiesMedia := jar.Cookies(uMedia)
	var foundMedia bool
	for _, c := range cookiesMedia {
		if c.Name == "arl" && c.Value == "prod_arl_token" {
			foundMedia = true
			break
		}
	}
	if !foundMedia {
		t.Errorf("expected arl cookie in jar for %s, got: %v", uMedia, cookiesMedia)
	}

	// 2. Test mock HTTP endpoint
	mockURL := "http://127.0.0.1:54321/ajax/gw-light.php"
	setARLCookie(jar, mockURL, "mock_arl_token")

	uMock, _ := url.Parse(mockURL)
	cookiesMock := jar.Cookies(uMock)
	var foundMock bool
	for _, c := range cookiesMock {
		if c.Name == "arl" && c.Value == "mock_arl_token" {
			foundMock = true
			break
		}
	}
	if !foundMock {
		t.Errorf("expected arl cookie in jar for %s, got: %v", mockURL, cookiesMock)
	}
}

func TestCoverURLResolution(t *testing.T) {
	prov := New(ClientOptions{})

	// 1. Direct HTTP URL
	httpURL := "https://example.com/cover.jpg"
	if u := prov.GetCoverURL(httpURL, 500); u != httpURL {
		t.Errorf("expected %q, got %q", httpURL, u)
	}

	// 2. MD5 32-hex image hash
	md5Hash := "5718f7c81c27e0b2417e2a4c45224f8a"
	uMD5 := prov.GetCoverURL(md5Hash, 500)
	if uMD5 != "https://cdn-images.dzcdn.net/images/cover/5718f7c81c27e0b2417e2a4c45224f8a/500x500.jpg" {
		t.Errorf("expected cdn-images URL for MD5 hash, got: %s", uMD5)
	}

	// 3. Numeric Album ID
	albumID := "302127"
	uAlbum := prov.GetCoverURL(albumID, 500)
	if uAlbum != "https://api.deezer.com/album/302127/image?size=big" {
		t.Errorf("expected api.deezer.com redirect URL for numeric ID, got: %s", uAlbum)
	}
}

