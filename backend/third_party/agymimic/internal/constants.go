// Package internal contains constants extracted from agy.exe (Antigravity CLI 1.0.2).
// Verified by:
//   - direct binary string analysis (IDA Pro IDB at agy.exe.i64)
//   - live wire capture via Frida hook on net_http.NewRequestWithContext
//   - cross-reference with NoeFabris/opencode-antigravity-auth
package internal

// OAuth client credentials baked into agy.exe.
// These belong to the "installed application" Google OAuth project that the
// Antigravity CLI itself ships with; same pair is in CLIProxyAPI's
// constants.go and NoeFabris's constants.ts.
const (
	OAuthClientID     = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com"
	OAuthClientSecret = "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"
	OAuthRedirectPort = 51121
	OAuthRedirectURI  = "http://localhost:51121/oauth-callback"
)

// Scopes requested by agy.exe during OAuth.
// `cclog` and `experimentsandconfigs` are the Antigravity-specific scopes
// that distinguish agy from a generic Google OAuth client (e.g. Gemini CLI).
var OAuthScopes = []string{
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/userinfo.email",
	"https://www.googleapis.com/auth/userinfo.profile",
	"https://www.googleapis.com/auth/cclog",
	"https://www.googleapis.com/auth/experimentsandconfigs",
}

// Google OAuth/identity endpoints.
const (
	GoogleAuthEndpoint    = "https://accounts.google.com/o/oauth2/v2/auth"
	GoogleTokenEndpoint   = "https://oauth2.googleapis.com/token"
	GoogleUserInfoV1      = "https://www.googleapis.com/oauth2/v1/userinfo?alt=json"
	GoogleUserInfoV2      = "https://www.googleapis.com/oauth2/v2/userinfo"
)

// Cloud Code (Antigravity) backend endpoints, in fallback order.
// agy.exe defaults to Daily Sandbox; Prod is the legacy fallback.
const (
	EndpointDailySandbox    = "https://daily-cloudcode-pa.sandbox.googleapis.com"
	EndpointAutopushSandbox = "https://autopush-cloudcode-pa.sandbox.googleapis.com"
	EndpointProd            = "https://cloudcode-pa.googleapis.com"
)

// EndpointFallbacks is the order agy tries when one endpoint fails.
var EndpointFallbacks = []string{
	EndpointDailySandbox,
	EndpointAutopushSandbox,
	EndpointProd,
}

// LoadEndpoints is the order for `loadCodeAssist` (project discovery).
// Prod is best for managed project resolution.
var LoadEndpoints = []string{
	EndpointProd,
	EndpointDailySandbox,
	EndpointAutopushSandbox,
}

// API paths.
const (
	PathLoadCodeAssist     = "/v1internal:loadCodeAssist"
	PathOnboardUser        = "/v1internal:onboardUser"
	PathGenerateContent    = "/v1internal:generateContent"
	PathStreamGenerate     = "/v1internal:streamGenerateContent?alt=sse"
	PathCountTokens        = "/v1internal:countTokens"
	PathListExperiments    = "/v1internal:listExperiments"
	PathFetchUserInfo      = "/v1internal:fetchUserInfo"
	PathFetchAvailableModels = "/v1internal:fetchAvailableModels"
	PathRecordTrajectory   = "/v1internal:recordTrajectoryAnalytics"
)

// Default version baked into agy.exe (real binary on disk is 1.0.2, but
// the Antigravity desktop fork advertises higher version numbers — Google
// honors both per live testing).
const DefaultAntigravityVersion = "1.21.9"

// FallbackProjectID — hardcoded last-resort project; only used if loadCodeAssist
// AND onboardUser both fail to produce a cloudaicompanionProject for the user.
const FallbackProjectID = "rising-fact-p41fc"

// Unleash feature-flag service used for organic traffic mimicry.
const (
	UnleashHost         = "https://antigravity-unleash.goog"
	UnleashAuthHeader   = "*:production.e44558998bfc35ea9584dc65858e4485fdaa5d7ef46903e0c67712d1"
	UnleashAppName      = "codeium-language-server"
	UnleashSDKVersion   = "unleash-client-go:4.5.0"
	UnleashSpecVersion  = "4.3.1"
	UnleashPlatformName = "go"
	UnleashInterval     = 60
)

// UnleashStrategies — strategies field advertised by the SDK in /register.
var UnleashStrategies = []string{
	"default",
	"applicationHostname",
	"gradualRolloutRandom",
	"gradualRolloutSessionId",
	"gradualRolloutUserId",
	"remoteAddress",
	"userWithId",
	"flexibleRollout",
}
