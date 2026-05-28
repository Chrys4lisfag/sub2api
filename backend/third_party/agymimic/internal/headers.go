package internal

import (
	"fmt"
	"net/http"
	"runtime"
)

// Platform string used in Client-Metadata + UA tail (per agy.exe runtime).
func platformTag() string {
	switch runtime.GOOS {
	case "windows":
		return "WINDOWS"
	case "darwin":
		return "MACOS"
	case "linux":
		return "LINUX"
	default:
		return "UNKNOWN"
	}
}

func uaTail() string {
	// agy.exe and CLIProxyAPI both ALWAYS advertise darwin/arm64 in their UA
	// regardless of actual OS. Wrong suffix → backend returns
	//   "This version of Antigravity is no longer supported" (despite version being current).
	return "darwin/arm64"
}

// AntigravityUA returns the short runtime UA used by generate/stream/model-list calls.
// Matches agy.exe's `antigravity/<ver> <os>/<arch>` format.
func AntigravityUA(version string) string {
	if version == "" {
		version = LatestAntigravityVersion()
	}
	return fmt.Sprintf("antigravity/%s %s", version, uaTail())
}

// LoadCodeAssistUA returns the long Electron-style UA used for loadCodeAssist
// and other control-plane calls. agy.exe sends this form for "Mozilla-looking"
// requests so the backend treats the call as coming from the desktop IDE.
func LoadCodeAssistUA(version string) string {
	if version == "" {
		version = LatestAntigravityVersion()
	}
	return fmt.Sprintf(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "+
			"Antigravity/%s Chrome/138.0.7204.235 Electron/37.3.1 Safari/537.36",
		version,
	)
}

// ClientMetadata returns the `Client-Metadata` header value agy sends.
func ClientMetadata() string {
	return fmt.Sprintf(`{"ideType":"ANTIGRAVITY","platform":"%s","pluginType":"GEMINI"}`, platformTag())
}

// SetAntigravityHeaders writes the agy.exe-style header set onto req.
// Pass version="" to use DefaultAntigravityVersion.
func SetAntigravityHeaders(req *http.Request, accessToken, version string) {
	req.Header.Set("User-Agent", AntigravityUA(version))
	req.Header.Set("X-Goog-Api-Client", "google-cloud-sdk vscode_cloudshelleditor/0.1")
	req.Header.Set("Client-Metadata", ClientMetadata())
	req.Header.Set("Content-Type", "application/json")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
}

// SetLoadCodeAssistHeaders writes the long-UA header set used by agy for
// loadCodeAssist / onboardUser calls.
func SetLoadCodeAssistHeaders(req *http.Request, accessToken, version string) {
	req.Header.Set("User-Agent", LoadCodeAssistUA(version))
	req.Header.Set("X-Goog-Api-Client", "google-cloud-sdk vscode_cloudshelleditor/0.1")
	req.Header.Set("Client-Metadata", ClientMetadata())
	req.Header.Set("Content-Type", "application/json")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
}
