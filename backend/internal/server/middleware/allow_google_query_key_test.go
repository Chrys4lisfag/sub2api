package middleware

import "testing"

// TestAllowGoogleQueryKey_NativePrefix locks in the 2026-06-18 bug fix:
// /antigravity-native/v1beta now accepts the Gemini-style ?key=<API_KEY>
// query-param auth, on parity with /v1beta and /antigravity/v1beta.
//
// google.genai SDKs (Python/JS/Go) automatically append ?key=… whenever
// the `api_key` field is set on the Client. Before the fix, hindsight
// (and any other google.genai consumer) silently 401'd against the
// native route while succeeding on the other two. Bearer auth always
// worked here — only the query-param fallback was gated out.
func TestAllowGoogleQueryKey_NativePrefix(t *testing.T) {
	cases := []struct {
		path string
		want bool
		why  string
	}{
		{"/v1beta/models/gemini-3.5-flash:generateContent", true, "stock Gemini route"},
		{"/antigravity/v1beta/models/gemini-3.5-flash:generateContent", true, "legacy AG route"},
		{"/antigravity-native/v1beta/models/gemini-3.5-flash:generateContent", true, "native AG route — the fix"},
		{"/antigravity-native/v1beta/models/gemini-3.5-flash:streamGenerateContent", true, "native AG streaming"},
		{"/antigravity-native/v1/messages", false, "anthropic-shape route — key in URL would leak through proxy access logs"},
		{"/v1/chat/completions", false, "openai-shape route — same reason"},
		{"/", false, "frontend root"},
		{"/api/v1/admin/groups", false, "admin route"},
	}
	for _, c := range cases {
		got := allowGoogleQueryKey(c.path)
		if got != c.want {
			t.Errorf("allowGoogleQueryKey(%q): want %v, got %v  (%s)", c.path, c.want, got, c.why)
		}
	}
}
