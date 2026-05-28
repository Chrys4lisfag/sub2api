package internal

import (
	"net/http"
	"strings"
	"testing"
)

func TestAntigravityUA(t *testing.T) {
	ua := AntigravityUA("1.21.9")
	if !strings.HasPrefix(ua, "antigravity/1.21.9 ") {
		t.Fatalf("UA prefix wrong: %q", ua)
	}
}

func TestLoadCodeAssistUAShape(t *testing.T) {
	ua := LoadCodeAssistUA("1.21.9")
	for _, want := range []string{
		"Mozilla/5.0",
		"Antigravity/1.21.9",
		"Chrome/138.0.7204.235",
		"Electron/37.3.1",
	} {
		if !strings.Contains(ua, want) {
			t.Errorf("LoadCodeAssistUA missing %q: %s", want, ua)
		}
	}
}

func TestClientMetadataShape(t *testing.T) {
	m := ClientMetadata()
	for _, want := range []string{
		`"ideType":"ANTIGRAVITY"`,
		`"pluginType":"GEMINI"`,
		`"platform":"`, // followed by WINDOWS/MACOS/LINUX
	} {
		if !strings.Contains(m, want) {
			t.Errorf("ClientMetadata missing %q: %s", want, m)
		}
	}
}

func TestSetAntigravityHeaders(t *testing.T) {
	req, _ := http.NewRequest("POST", "https://example/", nil)
	SetAntigravityHeaders(req, "ya29.test", "")
	if got := req.Header.Get("Authorization"); got != "Bearer ya29.test" {
		t.Errorf("Authorization wrong: %q", got)
	}
	if got := req.Header.Get("X-Goog-Api-Client"); got != "google-cloud-sdk vscode_cloudshelleditor/0.1" {
		t.Errorf("X-Goog-Api-Client wrong: %q", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type wrong: %q", got)
	}
	if !strings.HasPrefix(req.Header.Get("User-Agent"), "antigravity/") {
		t.Errorf("User-Agent wrong: %q", req.Header.Get("User-Agent"))
	}
	if !strings.HasPrefix(req.Header.Get("Client-Metadata"), `{"ideType":"ANTIGRAVITY"`) {
		t.Errorf("Client-Metadata wrong: %q", req.Header.Get("Client-Metadata"))
	}
}
