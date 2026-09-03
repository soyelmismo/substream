package tidal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	OfficialHiFiClientID     = "fX2JxdmntZWK0ixT"
	OfficialHiFiClientSecret = "1Nm5AfDAjxrgJFJbKNWLeAyKGVGmINuXPPLHVXAvxAg="
	DeviceAuthUserAgent      = "Mozilla/5.0 (Linux; Android 8.0.0; SM-G965F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.109 Mobile Safari/537.36"
	TidalDefaultScope        = "r_usr+w_usr+w_sub"
)

type DeviceCodeResponse struct {
	DeviceCode              string `json:"deviceCode"`
	UserCode                string `json:"userCode"`
	VerificationURI         string `json:"verificationUri"`
	VerificationURIComplete string `json:"verificationUriComplete"`
	ExpiresIn               int    `json:"expiresIn"`
	Interval                int    `json:"interval"`
}

type TokenResponse struct {
	AccessToken  string      `json:"access_token"`
	RefreshToken string      `json:"refresh_token"`
	TokenType    string      `json:"token_type"`
	ExpiresIn    int         `json:"expires_in"`
	UserID       interface{} `json:"user_id"` // can be float64 or string
	User         struct {
		UserID int64 `json:"userId"`
	} `json:"user"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (tr *TokenResponse) GetUserIDStr() string {
	if tr.User.UserID != 0 {
		return fmt.Sprintf("%d", tr.User.UserID)
	}
	if tr.UserID != nil {
		switch v := tr.UserID.(type) {
		case float64:
			return fmt.Sprintf("%.0f", v)
		case string:
			return v
		}
	}
	return ""
}

func basicAuthHeader(clientID, clientSecret string) string {
	auth := clientID + ":" + clientSecret
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

// RequestDeviceAuthorization contacts auth.tidal.com to request an OAuth2 Device Code.
func RequestDeviceAuthorization(ctx context.Context) (*DeviceCodeResponse, error) {
	data := url.Values{}
	data.Set("client_id", OfficialHiFiClientID)
	data.Set("scope", TidalDefaultScope)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://auth.tidal.com/v1/oauth2/device_authorization", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", DeviceAuthUserAgent)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("device auth request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("device auth returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var dcr DeviceCodeResponse
	if err := json.Unmarshal(body, &dcr); err != nil {
		return nil, fmt.Errorf("unmarshal device auth response: %w", err)
	}

	if dcr.VerificationURIComplete != "" {
		if !strings.HasPrefix(dcr.VerificationURIComplete, "http://") && !strings.HasPrefix(dcr.VerificationURIComplete, "https://") {
			dcr.VerificationURIComplete = "https://" + dcr.VerificationURIComplete
		}
	} else if dcr.VerificationURI != "" {
		dcr.VerificationURIComplete = fmt.Sprintf("https://%s/%s", strings.TrimPrefix(dcr.VerificationURI, "https://"), dcr.UserCode)
	} else if dcr.UserCode != "" {
		dcr.VerificationURIComplete = "https://link.tidal.com/" + dcr.UserCode
	}

	if dcr.VerificationURI != "" && !strings.HasPrefix(dcr.VerificationURI, "http://") && !strings.HasPrefix(dcr.VerificationURI, "https://") {
		dcr.VerificationURI = "https://" + dcr.VerificationURI
	}

	return &dcr, nil
}

// PollForDeviceTokens polls auth.tidal.com once to check if the user has approved the device code.
// If approved, returns TokenResponse with refresh_token and access_token.
// If pending, returns nil, nil.
func PollForDeviceTokens(ctx context.Context, deviceCode string) (*TokenResponse, error) {
	data := url.Values{}
	data.Set("client_id", OfficialHiFiClientID)
	data.Set("device_code", deviceCode)
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	data.Set("scope", TidalDefaultScope)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://auth.tidal.com/v1/oauth2/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", basicAuthHeader(OfficialHiFiClientID, OfficialHiFiClientSecret))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", DeviceAuthUserAgent)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var tokResp TokenResponse
	if err := json.Unmarshal(body, &tokResp); err != nil {
		return nil, fmt.Errorf("unmarshal token response: %w", err)
	}

	if resp.StatusCode == http.StatusOK {
		if tokResp.AccessToken == "" {
			return nil, fmt.Errorf("token response missing access_token")
		}
		return &tokResp, nil
	}

	// Handled pending states
	if tokResp.Error == "authorization_pending" || tokResp.Error == "slow_down" {
		return nil, nil // Still waiting for user
	}

	return nil, fmt.Errorf("token request error (%s): %s", tokResp.Error, tokResp.ErrorDescription)
}

// RefreshAccessToken exchanges a valid refresh_token for a fresh access_token.
func RefreshAccessToken(ctx context.Context, refreshToken string) (*TokenResponse, error) {
	data := url.Values{}
	data.Set("client_id", OfficialHiFiClientID)
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", refreshToken)
	data.Set("scope", TidalDefaultScope)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://auth.tidal.com/v1/oauth2/token", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", basicAuthHeader(OfficialHiFiClientID, OfficialHiFiClientSecret))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", DeviceAuthUserAgent)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("refresh token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var tokResp TokenResponse
	if err := json.Unmarshal(body, &tokResp); err != nil {
		return nil, fmt.Errorf("unmarshal refresh response: %w", err)
	}

	if tokResp.AccessToken == "" {
		return nil, fmt.Errorf("no access_token in refresh response")
	}

	return &tokResp, nil
}

// FetchAccountSubscription checks Tidal's subscription endpoint for an account
// and returns the subscription type (e.g. HIFI, PREMIUM, FREE), highest sound quality, and quality tier.
func FetchAccountSubscription(ctx context.Context, userID, accessToken string) (string, string, int, error) {
	if userID == "" || accessToken == "" {
		return "PREMIUM", "HIGH", 50, fmt.Errorf("empty userID or accessToken")
	}

	u := fmt.Sprintf("https://api.tidal.com/v1/users/%s/subscription", userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "PREMIUM", "HIGH", 50, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", DeviceAuthUserAgent)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "PREMIUM", "HIGH", 50, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "PREMIUM", "HIGH", 50, fmt.Errorf("subscription endpoint returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var subResp struct {
		HighestSoundQuality string `json:"highestSoundQuality"`
		Status              string `json:"status"`
		Subscription        struct {
			Type string `json:"type"`
		} `json:"subscription"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&subResp); err != nil {
		return "PREMIUM", "HIGH", 50, err
	}

	subType := strings.ToUpper(strings.TrimSpace(subResp.Subscription.Type))
	highestQuality := strings.ToUpper(strings.TrimSpace(subResp.HighestSoundQuality))

	tier := 50 // High (320kbps AAC)
	if subType == "HIFI" || subType == "TIDAL_HI_FI" || subType == "HI_RES" || (highestQuality == "HI_RES" && subType != "PREMIUM") {
		tier = 80 // Lossless FLAC
		if highestQuality == "HI_RES" {
			tier = 100 // Hi-Res FLAC
		}
	} else if subType == "PREMIUM" {
		tier = 50 // 320kbps AAC
	} else if subType == "FREE" {
		tier = 30
	}

	return subType, highestQuality, tier, nil
}
