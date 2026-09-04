package ytmusic

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// RawCookie represents a single cookie from JSON or Netscape format
type RawCookie struct {
	Domain         string      `json:"domain"`
	ExpirationDate interface{} `json:"expirationDate"`
	HostOnly       bool        `json:"hostOnly"`
	HttpOnly       bool        `json:"httpOnly"`
	Name           string      `json:"name"`
	Path           string      `json:"path"`
	Secure         bool        `json:"secure"`
	Value          string      `json:"value"`
}

// ParseCookies parses cookie data in JSON, Netscape cookies.txt, or raw Cookie header format.
// It returns:
// - netscapeContent: string formatted as Netscape cookies.txt for yt-dlp
// - cookieHeader: string formatted as HTTP "Cookie: ..." header for InnerTube
// - count: number of valid cookies found
// - err: non-nil if no valid cookies could be extracted
func ParseCookies(input string) (netscapeContent string, cookieHeader string, count int, err error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", "", 0, fmt.Errorf("empty cookie data")
	}

	var cookies []RawCookie

	// 1. Try parsing as JSON array
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
		var jsonList []RawCookie
		if errJSON := json.Unmarshal([]byte(trimmed), &jsonList); errJSON == nil && len(jsonList) > 0 {
			for _, c := range jsonList {
				if c.Name != "" {
					if c.Domain == "" {
						c.Domain = ".youtube.com"
					}
					if c.Path == "" {
						c.Path = "/"
					}
					cookies = append(cookies, c)
				}
			}
		}
	}

	// 2. If not JSON, try parsing Netscape cookies.txt or line-based format
	if len(cookies) == 0 {
		lines := strings.Split(trimmed, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || (strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "#HttpOnly_")) {
				continue
			}
			parts := strings.Split(line, "\t")
			if len(parts) >= 7 {
				domain := parts[0]
				path := parts[2]
				secure := strings.EqualFold(parts[3], "TRUE")
				var exp float64
				fmt.Sscanf(parts[4], "%f", &exp)
				name := parts[5]
				val := parts[6]
				httpOnly := false
				if strings.HasPrefix(domain, "#HttpOnly_") {
					domain = strings.TrimPrefix(domain, "#HttpOnly_")
					httpOnly = true
				}
				cookies = append(cookies, RawCookie{
					Domain:         domain,
					ExpirationDate: exp,
					Path:           path,
					Secure:         secure,
					HttpOnly:       httpOnly,
					Name:           name,
					Value:          val,
				})
			}
		}
	}

	// 3. If still empty, try raw HTTP Cookie header format: "key=val; key2=val2"
	if len(cookies) == 0 && strings.Contains(trimmed, "=") {
		pairs := strings.Split(trimmed, ";")
		for _, p := range pairs {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			idx := strings.Index(p, "=")
			if idx > 0 {
				k := strings.TrimSpace(p[:idx])
				v := strings.TrimSpace(p[idx+1:])
				cookies = append(cookies, RawCookie{
					Domain:         ".youtube.com",
					Path:           "/",
					ExpirationDate: 0,
					Name:           k,
					Value:          v,
				})
			}
		}
	}

	if len(cookies) == 0 {
		return "", "", 0, fmt.Errorf("could not parse any valid cookies from input")
	}

	// Build Netscape format for yt-dlp
	var netscapeBuilder strings.Builder
	netscapeBuilder.WriteString("# Netscape HTTP Cookie File\n")
	netscapeBuilder.WriteString("# https://curl.haxx.se/rfc/cookie_spec.html\n")
	netscapeBuilder.WriteString("# This is a generated file! Do not edit.\n\n")

	// Build Cookie header for InnerTube
	var headerPairs []string

	for _, c := range cookies {
		domain := c.Domain
		if !strings.HasPrefix(domain, ".") && !strings.Contains(domain, "localhost") {
			domain = "." + domain
		}
		prefix := ""
		if c.HttpOnly {
			prefix = "#HttpOnly_"
		}
		subdomains := "TRUE"
		if !strings.HasPrefix(domain, ".") {
			subdomains = "FALSE"
		}
		path := c.Path
		if path == "" {
			path = "/"
		}
		secureStr := "FALSE"
		if c.Secure {
			secureStr = "TRUE"
		}

		expInt := int64(2147483647) // default far future
		switch v := c.ExpirationDate.(type) {
		case float64:
			if v > 0 {
				expInt = int64(math.Round(v))
			}
		case int64:
			if v > 0 {
				expInt = v
			}
		case int:
			if v > 0 {
				expInt = int64(v)
			}
		}

		netscapeBuilder.WriteString(fmt.Sprintf("%s%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			prefix, domain, subdomains, path, secureStr, expInt, c.Name, c.Value))

		headerPairs = append(headerPairs, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	return netscapeBuilder.String(), strings.Join(headerPairs, "; "), len(cookies), nil
}
