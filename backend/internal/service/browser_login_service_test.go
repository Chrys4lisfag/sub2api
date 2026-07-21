package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func mustBasicAuthUser(t *testing.T, r *http.Request) string {
	t.Helper()
	user, password, ok := r.BasicAuth()
	require.True(t, ok)
	require.Equal(t, "secret", password)
	return user
}
