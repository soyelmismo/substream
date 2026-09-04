package ytmusic

import (
	"strings"
	"testing"
)

func TestParseCookiesJSON(t *testing.T) {
	jsonInput := `[
		{
			"domain": ".youtube.com",
			"expirationDate": 1791297022,
			"hostOnly": false,
			"httpOnly": false,
			"name": "_gcl_au",
			"path": "/",
			"secure": false,
			"value": "1.1.1681329345.1783521022"
		},
		{
			"domain": ".youtube.com",
			"expirationDate": 1822976191.037675,
			"hostOnly": false,
			"httpOnly": true,
			"name": "__Secure-3PSID",
			"path": "/",
			"secure": true,
			"value": "g.a000CAnkvRk2KRaYspnCKgwj6P8b6poCnzpaJjGUX45nMC5S"
		}
	]`

	netscape, header, count, err := ParseCookies(jsonInput)
	if err != nil {
		t.Fatalf("ParseCookies failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 cookies, got %d", count)
	}
	if !strings.Contains(netscape, "# Netscape HTTP Cookie File") {
		t.Errorf("expected netscape header in output")
	}
	if !strings.Contains(netscape, "_gcl_au") || !strings.Contains(netscape, "__Secure-3PSID") {
		t.Errorf("expected cookie names in netscape output")
	}
	if !strings.Contains(header, "_gcl_au=") || !strings.Contains(header, "__Secure-3PSID=") {
		t.Errorf("expected cookie names in header output: %s", header)
	}
}

func TestParseCookiesNetscape(t *testing.T) {
	netscapeInput := `# Netscape HTTP Cookie File
.youtube.com	TRUE	/	FALSE	1791297022	SID	test_sid_value
#HttpOnly_.youtube.com	TRUE	/	TRUE	1822976191	HSID	test_hsid_value
`
	netscape, header, count, err := ParseCookies(netscapeInput)
	if err != nil {
		t.Fatalf("ParseCookies failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 cookies, got %d", count)
	}
	if !strings.Contains(header, "SID=test_sid_value") {
		t.Errorf("missing SID in header: %s", header)
	}
	if !strings.Contains(netscape, "#HttpOnly_.youtube.com") {
		t.Errorf("expected HttpOnly prefix in netscape output")
	}
}

func TestParseCookiesHeader(t *testing.T) {
	headerInput := "SID=abc123xyz; HSID=def456uvw; SSID=ghi789rst"
	_, header, count, err := ParseCookies(headerInput)
	if err != nil {
		t.Fatalf("ParseCookies failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 cookies, got %d", count)
	}
	if !strings.Contains(header, "SID=abc123xyz") {
		t.Errorf("expected SID in header")
	}
}
