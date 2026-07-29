package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrowserLoginDoJSONForwardsSessionID(t *testing.T) {
	const sessionID = "session-a"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, sessionID, r.Header.Get("X-Browser-Session-ID"))
		require.Equal(t, "admin", mustBasicAuthUser(t, r))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer server.Close()

	svc := &BrowserLoginService{}
	var result struct {
		OK bool `json:"ok"`
	}
	err := svc.doJSON(
		context.Background(),
		browserLoginCreds{baseURL: server.URL, user: "admin", pass: "secret"},
		http.MethodGet,
		"/session/result",
		sessionID,
		nil,
		&result,
	)
	require.NoError(t, err)
	require.True(t, result.OK)
}

func TestBrowserLoginDoJSONSanitizesBoundedUpstreamError(t *testing.T) {
	requestBody := map[string]any{
		"login":                  "person@example.test",
		"password":               "password-secret",
		"two_factor_import_code": "TWO-FACTOR-SECRET",
		"herosms_api_key":        "HEROSMS-SECRET",
	}
	credentials := browserLoginCreds{
		user: "panel-user",
		pass: "panel-password-secret",
	}
	const sessionID = "session-secret"
	leakedValues := []string{
		requestBody["login"].(string),
		requestBody["password"].(string),
		requestBody["two_factor_import_code"].(string),
		requestBody["herosms_api_key"].(string),
		credentials.user,
		credentials.pass,
		sessionID,
	}
	message := "activation rejected: " + strings.Join(leakedValues, " ") + strings.Repeat("x", 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"detail":     message,
			"debug_dump": "raw-body-marker",
		})
	}))
	defer server.Close()
	credentials.baseURL = server.URL

	err := (&BrowserLoginService{}).doJSON(
		context.Background(),
		credentials,
		http.MethodPost,
		"/session/google-autologin",
		sessionID,
		requestBody,
		nil,
	)

	var upstreamErr *BrowserLoginUpstreamError
	require.ErrorAs(t, err, &upstreamErr)
	require.Equal(t, http.StatusConflict, upstreamErr.StatusCode)
	require.LessOrEqual(t, len([]rune(upstreamErr.Error())), browserLoginErrorMessageLimit)
	require.Contains(t, upstreamErr.Error(), "[redacted]")
	require.NotContains(t, upstreamErr.Error(), "raw-body-marker")
	for _, secret := range leakedValues {
		require.NotContains(t, upstreamErr.Error(), secret)
	}
}

func mustBasicAuthUser(t *testing.T, r *http.Request) string {
	t.Helper()
	user, password, ok := r.BasicAuth()
	require.True(t, ok)
	require.Equal(t, "secret", password)
	return user
}
