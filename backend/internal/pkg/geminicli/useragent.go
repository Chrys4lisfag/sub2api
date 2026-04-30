// Package geminicli — User-Agent builder for the Gemini CLI flow.
package geminicli

import "fmt"

// BuildGeminiCLIUserAgent returns the compound User-Agent the real Gemini CLI emits
// on cloudcode-pa requests. Format with a model in scope:
//
//	GeminiCLI/{ver}/{model} (win32; x64; terminal) google-api-nodejs-client/{sdkVer}
//
// Without a model (e.g. loadCodeAssist / onboardUser), the model segment is dropped:
//
//	GeminiCLI/{ver} (win32; x64; terminal) google-api-nodejs-client/{sdkVer}
//
// Platform/arch are intentionally hardcoded to win32/x64 (Node.js naming) to mirror
// the canonical fingerprint Google's classifier expects, regardless of where this
// Go server actually runs. Surface is hardcoded `terminal` -- not `cloud-shell`,
// which would imply running inside Google Cloud Shell.
//
// Last verified against a Fiddler capture of Gemini CLI 0.40.1 on 2026-04-30.
func BuildGeminiCLIUserAgent(model string) string {
	const (
		platform = "win32"
		arch     = "x64"
		surface  = "terminal"
	)
	if model == "" {
		return fmt.Sprintf(
			"GeminiCLI/%s (%s; %s; %s) google-api-nodejs-client/%s",
			GeminiCLIVersion, platform, arch, surface, GoogleAPINodeClientVersion,
		)
	}
	return fmt.Sprintf(
		"GeminiCLI/%s/%s (%s; %s; %s) google-api-nodejs-client/%s",
		GeminiCLIVersion, model, platform, arch, surface, GoogleAPINodeClientVersion,
	)
}
