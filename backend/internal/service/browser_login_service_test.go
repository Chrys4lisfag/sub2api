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

type browserProfileAccountRepo struct {
	AccountRepository
	accounts map[int64]*Account
	updates  []int64
}

func (r *browserProfileAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	return r.accounts[id], nil
}

func (r *browserProfileAccountRepo) FindByExtraField(_ context.Context, key string, value any) ([]Account, error) {
	matches := make([]Account, 0)
	for _, account := range r.accounts {
		if account.Extra != nil && account.Extra[key] == value {
			matches = append(matches, *account)
		}
	}
	return matches, nil
}

func (r *browserProfileAccountRepo) UpdateExtra(_ context.Context, id int64, updates map[string]any) error {
	account := r.accounts[id]
	if account.Extra == nil {
		account.Extra = make(map[string]any)
	}
	for key, value := range updates {
		account.Extra[key] = value
	}
	r.updates = append(r.updates, id)
	return nil
}

func TestResolveBrowserProfileIDUsesPersistedAccountOwner(t *testing.T) {
	accountID := int64(11)
	repo := &browserProfileAccountRepo{accounts: map[int64]*Account{
		accountID: {ID: accountID, Extra: map[string]any{browserProfileExtraKey: "profile-11"}},
	}}
	svc := &BrowserLoginService{accountRepo: repo}

	profileID, err := svc.resolveBrowserProfileID(context.Background(), &BrowserLoginStartInput{
		AccountID: &accountID,
		ProfileID: "profile-from-another-account",
	})

	require.NoError(t, err)
	require.Equal(t, "profile-11", profileID)
	require.Empty(t, repo.updates)
}

func TestResolveBrowserProfileIDCreatesDistinctAccountProfiles(t *testing.T) {
	firstID := int64(21)
	secondID := int64(22)
	repo := &browserProfileAccountRepo{accounts: map[int64]*Account{
		firstID:  {ID: firstID},
		secondID: {ID: secondID},
	}}
	svc := &BrowserLoginService{accountRepo: repo}

	firstProfile, err := svc.resolveBrowserProfileID(context.Background(), &BrowserLoginStartInput{AccountID: &firstID})
	require.NoError(t, err)
	secondProfile, err := svc.resolveBrowserProfileID(context.Background(), &BrowserLoginStartInput{AccountID: &secondID})
	require.NoError(t, err)

	require.Equal(t, "account-21", firstProfile)
	require.Equal(t, "account-22", secondProfile)
	require.NotEqual(t, firstProfile, secondProfile)
	require.Equal(t, firstProfile, repo.accounts[firstID].Extra[browserProfileExtraKey])
	require.Equal(t, secondProfile, repo.accounts[secondID].Extra[browserProfileExtraKey])
}

func TestResolveBrowserProfileIDSplitsEveryLegacySharedOwner(t *testing.T) {
	firstID := int64(31)
	secondID := int64(32)
	repo := &browserProfileAccountRepo{accounts: map[int64]*Account{
		firstID:  {ID: firstID, Extra: map[string]any{browserProfileExtraKey: "legacy-shared"}},
		secondID: {ID: secondID, Extra: map[string]any{browserProfileExtraKey: "legacy-shared"}},
	}}
	svc := &BrowserLoginService{accountRepo: repo}

	firstProfile, err := svc.resolveBrowserProfileID(context.Background(), &BrowserLoginStartInput{AccountID: &firstID})
	require.NoError(t, err)
	secondProfile, err := svc.resolveBrowserProfileID(context.Background(), &BrowserLoginStartInput{AccountID: &secondID})
	require.NoError(t, err)

	require.Equal(t, "account-31", firstProfile)
	require.Equal(t, "account-32", secondProfile)
	require.Equal(t, firstProfile, repo.accounts[firstID].Extra[browserProfileExtraKey])
	require.Equal(t, secondProfile, repo.accounts[secondID].Extra[browserProfileExtraKey])
	require.ElementsMatch(t, []int64{firstID, secondID}, repo.updates)
}
