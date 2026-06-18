package service

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

// TestClassifyNativeUpstreamErr_InvalidGrant verifies the OAuth refresh
// failure path. agymimic surfaces a dead refresh_token as a wrapped
// error whose string contains "invalid_grant" — that must materialise
// as a 401 UNAUTHENTICATED failover so admins see a clear "re-auth"
// signal rather than the silent 200/empty the gateway used to emit
// when ForwardGemini returned naked fmt.Errorf.
func TestClassifyNativeUpstreamErr_InvalidGrant(t *testing.T) {
	src := errors.New("refresh: refresh: 400: {\n  \"error\": \"invalid_grant\",\n  \"error_description\": \"Bad Request\"\n}")
	got := classifyNativeUpstreamErr(src, "upstream")
	if got == nil {
		t.Fatal("classify returned nil")
	}
	if got.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: want 401, got %d", got.StatusCode)
	}
	if got.RetryableOnSameAccount {
		t.Error("RetryableOnSameAccount should be false for invalid_grant — the token is dead, no point retrying this account")
	}
	if !got.PassthroughVerbatim {
		t.Error("PassthroughVerbatim should be true so admins see the upstream message")
	}
	body := string(got.ResponseBody)
	if !strings.Contains(body, "UNAUTHENTICATED") {
		t.Errorf("body should carry UNAUTHENTICATED status, got: %s", body)
	}
	if !strings.Contains(body, "re-authentication") {
		t.Errorf("body should hint at re-authentication, got: %s", body)
	}
	if !strings.Contains(body, "invalid_grant") {
		t.Errorf("body should echo upstream invalid_grant string (escaped), got: %s", body)
	}
	if got.ResponseHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type: want application/json, got %q", got.ResponseHeaders.Get("Content-Type"))
	}
}

// TestClassifyNativeUpstreamErr_GenericNetwork verifies the non-OAuth
// branch: generic upstream failures (DNS, TLS, connection reset) get
// a 502 Bad Gateway with RetryableOnSameAccount=true so the failover
// loop retries the same account once before demoting it.
func TestClassifyNativeUpstreamErr_GenericNetwork(t *testing.T) {
	src := errors.New("dial tcp 142.250.184.10:443: connect: connection refused")
	got := classifyNativeUpstreamErr(src, "upstream")
	if got == nil {
		t.Fatal("classify returned nil")
	}
	if got.StatusCode != http.StatusBadGateway {
		t.Errorf("status: want 502, got %d", got.StatusCode)
	}
	if !got.RetryableOnSameAccount {
		t.Error("RetryableOnSameAccount should be true for transient network errors")
	}
	body := string(got.ResponseBody)
	if !strings.Contains(body, "UNAVAILABLE") {
		t.Errorf("body should carry UNAVAILABLE status, got: %s", body)
	}
	if !strings.Contains(body, "connection refused") {
		t.Errorf("body should echo upstream error, got: %s", body)
	}
}

// TestClassifyNativeUpstreamErr_StageInBody ensures the `stage` arg
// surfaces in the user-visible error message so a getClient-time
// failure (token store unreachable) vs. an upstream-time failure (real
// HTTP roundtrip blew up) can be told apart from the response body
// alone, without needing access to docker logs.
func TestClassifyNativeUpstreamErr_StageInBody(t *testing.T) {
	src := errors.New("oauth store: connection refused")
	got := classifyNativeUpstreamErr(src, "getClient")
	if !strings.Contains(string(got.ResponseBody), "getClient") {
		t.Errorf("body should include stage=getClient, got: %s", string(got.ResponseBody))
	}
}

// TestJsonEscape covers the four characters that break a JSON string
// literal. The function is used by classifyNativeUpstreamErr's fmt.Sprintf
// body builder; a regression here would emit malformed JSON and clients
// would see a parse error instead of the upstream diagnostic.
func TestJsonEscape(t *testing.T) {
	cases := []struct {
		in, out string
	}{
		{`hello`, `hello`},
		{`a"b`, `a\"b`},
		{`a\b`, `a\\b`},
		{"line1\nline2", `line1\nline2`},
		{"\r\n", `\r\n`},
		{`mix "x" \n` + "\n", `mix \"x\" \\n\n`},
	}
	for _, c := range cases {
		got := jsonEscape(c.in)
		if got != c.out {
			t.Errorf("jsonEscape(%q): want %q, got %q", c.in, c.out, got)
		}
	}
}
