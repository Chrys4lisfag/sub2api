package internal

import (
	"fmt"
	"net/http"
	"runtime"
)

// Real agy.exe wire fingerprint (verified empirically via Frida capture of
// crypto/tls.Conn.Write to daily-cloudcode-pa.googleapis.com, May 2026):
//
//   POST /v1internal:streamGenerateContent?alt=sse HTTP/1.1
//   Host: daily-cloudcode-pa.googleapis.com
//   User-Agent: antigravity/cli/<version> <os>/<arch>
//   Transfer-Encoding: chunked
//   Authorization: Bearer ya29.<token>
//   Content-Type: application/json
//   Accept-Encoding: gzip
//
// Notably absent (earlier versions of this file added these — they are NOT
// part of agy's wire and may even trigger upstream anti-bot heuristics):
//   - Client-Metadata
//   - X-Goog-Api-Client

// uaTail returns the {os}/{arch} suffix matching the host runtime — agy
// uses the real OS (windows/amd64 on Windows, darwin/arm64 on macOS, etc.)
// not a fixed value. Earlier comments claimed darwin/arm64 was mandatory;
// that was a misread — daily-cloudcode-pa accepts any standard combo.
func uaTail() string {
	osTag := runtime.GOOS
	switch osTag {
	case "darwin", "linux", "windows":
		// keep as-is
	default:
		osTag = "linux"
	}
	archTag := runtime.GOARCH
	switch archTag {
	case "amd64", "arm64", "arm", "386":
		// keep
	default:
		archTag = "amd64"
	}
	return osTag + "/" + archTag
}

// AntigravityUA returns the runtime UA agy.exe uses for cloudcode-pa
// requests: "antigravity/cli/<version> <os>/<arch>".
//
// Note the inserted "/cli/" segment — agy embeds the surface type
// (cli vs desktop) in the UA. We always identify as cli since agymimic
// has no UI/IDE counterpart.
func AntigravityUA(version string) string {
	if version == "" {
		version = LatestAntigravityVersion()
	}
	return fmt.Sprintf("antigravity/cli/%s %s", version, uaTail())
}

// LoadCodeAssistUA — alias of AntigravityUA. Earlier revisions used a
// long Mozilla/Electron UA for loadCodeAssist/onboardUser thinking the
// backend gated those endpoints on a "desktop IDE" UA. The Frida capture
// shows agy sends the SAME short UA for all v1internal:* paths.
func LoadCodeAssistUA(version string) string {
	return AntigravityUA(version)
}

// SetAntigravityHeaders writes the agy.exe-style header set onto req.
// Pass version="" to use the live-refreshed Antigravity version.
//
// Wire-verified header set (May 2026):
//   - User-Agent
//   - Content-Type: application/json
//   - Authorization: Bearer <token>  (when accessToken provided)
//   - Accept-Encoding: gzip
//
// We intentionally do NOT set Client-Metadata or X-Goog-Api-Client —
// real agy doesn't, and adding them changed nothing functionally while
// diverging from the captured wire.
func SetAntigravityHeaders(req *http.Request, accessToken, version string) {
	req.Header.Set("User-Agent", AntigravityUA(version))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
}

// SetLoadCodeAssistHeaders — alias of SetAntigravityHeaders. Same header
// set per wire capture; earlier divergence was speculative.
func SetLoadCodeAssistHeaders(req *http.Request, accessToken, version string) {
	SetAntigravityHeaders(req, accessToken, version)
}
