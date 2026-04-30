// Package geminicli provides helpers for interacting with Gemini CLI tools.
package geminicli

import "time"

const (
	AIStudioBaseURL  = "https://generativelanguage.googleapis.com"
	GeminiCliBaseURL = "https://cloudcode-pa.googleapis.com"

	AuthorizeURL = "https://accounts.google.com/o/oauth2/v2/auth"
	TokenURL     = "https://oauth2.googleapis.com/token"

	// AIStudioOAuthRedirectURI is the default redirect URI used for AI Studio OAuth.
	// This matches the "copy/paste callback URL" flow used by OpenAI OAuth in this project.
	// Note: You still need to register this redirect URI in your Google OAuth client
	// unless you use an OAuth client type that permits localhost redirect URIs.
	AIStudioOAuthRedirectURI = "http://localhost:1455/auth/callback"

	// DefaultScopes for Code Assist (includes cloud-platform for API access plus userinfo scopes)
	// Required by Google's Code Assist API.
	DefaultCodeAssistScopes = "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile email profile openid"

	// DefaultScopes for AI Studio (uses generativelanguage API with OAuth)
	// Reference: https://ai.google.dev/gemini-api/docs/oauth
	// For regular Google accounts, supports API calls to generativelanguage.googleapis.com
	// Note: Google Auth platform currently documents the OAuth scope as
	// https://www.googleapis.com/auth/generative-language.retriever (often with cloud-platform).
	DefaultAIStudioScopes = "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/generative-language.retriever"

	// DefaultGoogleOneScopes (DEPRECATED, no longer used)
	// Google One now always uses the built-in Gemini CLI client with DefaultCodeAssistScopes.
	// This constant is kept for backward compatibility but is not actively used.
	DefaultGoogleOneScopes = "https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/generative-language.retriever https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile"

	// GeminiCLIRedirectURI is the redirect URI used by Gemini CLI for Code Assist OAuth.
	GeminiCLIRedirectURI = "https://codeassist.google.com/authcode"

	// GeminiCLIOAuthClientID/Secret are the public OAuth client credentials used by Google Gemini CLI.
	// They enable the "login without creating your own OAuth client" experience, but Google may
	// restrict which scopes are allowed for this client.
	GeminiCLIOAuthClientID     = "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com"
	GeminiCLIOAuthClientSecret = "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"

	// GeminiCLIOAuthClientSecretEnv is the environment variable name for the built-in client secret.
	GeminiCLIOAuthClientSecretEnv = "GEMINI_CLI_OAUTH_CLIENT_SECRET"

	SessionTTL = 30 * time.Minute

	// GeminiCLIVersion is the CLI version we mimic in the User-Agent.
	// Bump alongside upstream pinning. Last verified against a real 0.40.1 wire capture.
	GeminiCLIVersion = "0.40.1"

	// GoogleAPINodeClientVersion is the trailing google-api-nodejs-client token in the
	// compound User-Agent that real Gemini CLI emits on cloudcode-pa requests, and the
	// stand-alone UA used on oauth2.googleapis.com/token requests.
	GoogleAPINodeClientVersion = "9.15.1"

	// GoogleAPIClientHeader is the value sent for the x-goog-api-client header on
	// Code Assist + OAuth token endpoint requests.
	GoogleAPIClientHeader = "gl-node/24.12.0"
)

// GeminiCLIUserAgent is the compound User-Agent without a model segment.
// Prefer BuildGeminiCLIUserAgent(model) at call sites that have the request model
// in scope; fall back to this var when no model is available (e.g. loadCodeAssist).
var GeminiCLIUserAgent = BuildGeminiCLIUserAgent("")
