package service

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestCleanGeminiNativeThoughtSignatures_ReplacesNestedThoughtSignatures(t *testing.T) {
	input := []byte(`{
		"contents": [
			{
				"role": "user",
				"parts": [{"text": "hello"}]
			},
			{
				"role": "model",
				"parts": [
					{"text": "thinking", "thought": true, "thoughtSignature": "sig_1"},
					{"functionCall": {"name": "toolA", "args": {"k": "v"}}, "thoughtSignature": "sig_2"}
				]
			}
		],
		"cachedContent": {
			"parts": [{"text": "cached", "thoughtSignature": "sig_3"}]
		},
		"signature": "keep_me"
	}`)

	cleaned := CleanGeminiNativeThoughtSignatures(input)

	var got map[string]any
	require.NoError(t, json.Unmarshal(cleaned, &got))

	require.NotContains(t, string(cleaned), `"thoughtSignature":"sig_1"`)
	require.NotContains(t, string(cleaned), `"thoughtSignature":"sig_2"`)
	require.NotContains(t, string(cleaned), `"thoughtSignature":"sig_3"`)
	require.Contains(t, string(cleaned), `"thoughtSignature":"`+antigravity.DummyThoughtSignature+`"`)
	require.Contains(t, string(cleaned), `"signature":"keep_me"`)
}

func TestCleanGeminiNativeThoughtSignatures_CleansPrebuiltEnvelope(t *testing.T) {
	input := []byte(`{"model":"gemini-3.6-flash-high","userAgent":"antigravity","request":{"contents":[{"parts":[{"text":"thinking","thoughtSignature":"stale"}]}]}}`)
	cleaned := CleanGeminiNativeThoughtSignatures(input)
	require.True(t, hasGeminiNativeThoughtSignature(input))
	require.Contains(t, string(cleaned), `"thoughtSignature":"`+antigravity.DummyThoughtSignature+`"`)
	require.NotContains(t, string(cleaned), `"thoughtSignature":"stale"`)
}

func TestCleanGeminiNativeThoughtSignatures_InvalidJSONReturnsOriginal(t *testing.T) {
	input := []byte(`{"contents":[invalid-json]}`)

	cleaned := CleanGeminiNativeThoughtSignatures(input)

	require.Equal(t, input, cleaned)
}

func TestCleanGeminiNativeThoughtSignatures_PreservesToolArguments(t *testing.T) {
	input := []byte(`{"contents":[{"parts":[{"functionCall":{"name":"tool","args":{"thoughtSignature":"user-data"}},"thoughtSignature":"stale-part"}]}],"metadata":{"thoughtSignature":"metadata-value"}}`)
	cleaned := CleanGeminiNativeThoughtSignatures(input)

	var got map[string]any
	require.NoError(t, json.Unmarshal(cleaned, &got))
	contents := got["contents"].([]any)
	part := contents[0].(map[string]any)["parts"].([]any)[0].(map[string]any)
	require.Equal(t, antigravity.DummyThoughtSignature, part["thoughtSignature"])
	args := part["functionCall"].(map[string]any)["args"].(map[string]any)
	require.Equal(t, "user-data", args["thoughtSignature"])
	require.Equal(t, "metadata-value", got["metadata"].(map[string]any)["thoughtSignature"])
}

func TestCleanGeminiNativeThoughtSignatures_PreservesLargeIntegers(t *testing.T) {
	input := []byte(`{"contents":[{"parts":[{"text":"thinking","thoughtSignature":"stale"},{"functionCall":{"name":"tool","args":{"large":9007199254740993}}}]}]}`)
	cleaned := CleanGeminiNativeThoughtSignatures(input)

	require.Contains(t, string(cleaned), `"large":9007199254740993`)
	require.Contains(t, string(cleaned), `"thoughtSignature":"`+antigravity.DummyThoughtSignature+`"`)
}

func TestCleanGeminiNativeThoughtSignatures_ReplacesEscapedPartKey(t *testing.T) {
	input := []byte(`{"contents":[{"parts":[{"\u0074houghtSignature":"stale"}]}]}`)
	cleaned := CleanGeminiNativeThoughtSignatures(input)
	require.Contains(t, string(cleaned), `"thoughtSignature":"`+antigravity.DummyThoughtSignature+`"`)
}

func TestCleanGeminiNativeThoughtSignatures_NoSchemaSignatureReturnsOriginal(t *testing.T) {
	input := []byte(`{"functionCall":{"args":{"thoughtSignature":"user-data"}}}`)
	require.Equal(t, input, CleanGeminiNativeThoughtSignatures(input))
}

func TestLowerGeminiNativeThinkingForSemanticRetry_LowersOneTierAtATime(t *testing.T) {
	input := []byte(`{
		"generationConfig": {
			"temperature": 0.7,
			"thinkingConfig": {
				"thinkingLevel": "HIGH",
				"thinkingBudget": 8192,
				"includeThoughts": true
			}
		},
		"contents": [{"role": "user", "parts": [{"text": "hello"}]}]
	}`)

	medium, changed := LowerGeminiNativeThinkingForSemanticRetry(input)
	require.True(t, changed)
	require.NotEqual(t, input, medium)

	var got map[string]any
	require.NoError(t, json.Unmarshal(medium, &got))
	generationConfig := got["generationConfig"].(map[string]any)
	thinkingConfig := generationConfig["thinkingConfig"].(map[string]any)
	require.Equal(t, "MEDIUM", thinkingConfig["thinkingLevel"])
	require.NotContains(t, thinkingConfig, "thinkingBudget")
	require.Equal(t, true, thinkingConfig["includeThoughts"])
	require.Equal(t, 0.7, generationConfig["temperature"])

	low, changed := LowerGeminiNativeThinkingForSemanticRetry(medium)
	require.True(t, changed)
	require.NoError(t, json.Unmarshal(low, &got))
	generationConfig = got["generationConfig"].(map[string]any)
	thinkingConfig = generationConfig["thinkingConfig"].(map[string]any)
	require.Equal(t, "LOW", thinkingConfig["thinkingLevel"])
	require.NotContains(t, thinkingConfig, "thinkingBudget")

	unchanged, changed := LowerGeminiNativeThinkingForSemanticRetry(low)
	require.False(t, changed)
	require.Equal(t, low, unchanged)
}

func TestLowerGeminiNativeThinkingForSemanticRetry_PreservesLargeIntegersAndToolArgs(t *testing.T) {
	input := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH","thinkingBudget":8192}},"contents":[{"parts":[{"functionCall":{"name":"tool","args":{"large":9007199254740993,"thinkingBudget":7777}}}]}]}`)
	lowered, changed := LowerGeminiNativeThinkingForSemanticRetry(input)
	require.True(t, changed)
	require.Contains(t, string(lowered), `"large":9007199254740993`)
	require.Contains(t, string(lowered), `"thinkingBudget":7777`)
	require.NotContains(t, string(lowered), `"thinkingBudget":8192`)
}

func TestLowerGeminiNativeThinkingForSemanticRetry_SupportsSDKConfigAndSnakeCase(t *testing.T) {
	cases := []struct{ name, input, levelPath, budgetPath string }{
		{name: "sdk config", input: `{"config":{"thinkingConfig":{"thinkingLevel":"HIGH","thinkingBudget":8192}}}`, levelPath: "config.thinkingConfig.thinkingLevel", budgetPath: "config.thinkingConfig.thinkingBudget"},
		{name: "nested snake case", input: `{"request":{"config":{"thinkingConfig":{"thinking_level":"HIGH","thinking_budget":8192}}}}`, levelPath: "request.config.thinkingConfig.thinking_level", budgetPath: "request.config.thinkingConfig.thinking_budget"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lowered, changed := LowerGeminiNativeThinkingForSemanticRetry([]byte(tc.input))
			require.True(t, changed)
			require.Equal(t, "MEDIUM", gjson.GetBytes(lowered, tc.levelPath).String())
			require.False(t, gjson.GetBytes(lowered, tc.budgetPath).Exists())
		})
	}
}

func TestLowerGeminiNativeThinkingForSemanticRetry_ProducesMediumAGYWireShape(t *testing.T) {
	high := []byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH","includeThoughts":true}}}`)
	medium, changed := LowerGeminiNativeThinkingForSemanticRetry(high)
	require.True(t, changed)

	wireModel := antigravity.ResolveWireFromBody("gemini-3.6-flash", medium)
	require.Equal(t, "gemini-3.6-flash-medium", wireModel)

	wrapped, err := wrapNativeV1Internal("project", wireModel, medium)
	require.NoError(t, err)
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(wrapped, &envelope))
	require.Equal(t, "gemini-3.6-flash-medium", envelope["model"])
	request := envelope["request"].(map[string]any)
	generationConfig := request["generationConfig"].(map[string]any)
	thinkingConfig := generationConfig["thinkingConfig"].(map[string]any)
	require.Equal(t, "MEDIUM", thinkingConfig["thinkingLevel"])
	require.Equal(t, float64(4000), thinkingConfig["thinkingBudget"])
}

func TestLowerGeminiNativeThinkingForSemanticRetry_NestedEnvelope(t *testing.T) {
	input := []byte(`{
		"project": "project",
		"request": {
			"generationConfig": {
				"thinkingConfig": {
					"thinkingLevel": "high",
					"includeThoughts": true
				}
			}
		}
	}`)

	lowered, changed := LowerGeminiNativeThinkingForSemanticRetry(input)
	require.True(t, changed)

	var got map[string]any
	require.NoError(t, json.Unmarshal(lowered, &got))
	request := got["request"].(map[string]any)
	generationConfig := request["generationConfig"].(map[string]any)
	thinkingConfig := generationConfig["thinkingConfig"].(map[string]any)
	require.Equal(t, "MEDIUM", thinkingConfig["thinkingLevel"])
	require.Equal(t, "project", got["project"])
}

func TestLowerGeminiNativeThinkingForSemanticRetry_NonLowerableReturnsOriginal(t *testing.T) {
	for _, input := range [][]byte{
		[]byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"LOW"}}}`),
		[]byte(`{"generationConfig":{"thinkingConfig":{"thinkingLevel":"MINIMAL"}}}`),
		[]byte(`{"generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`),
		[]byte(`{"contents":[]}`),
		[]byte(`{"generationConfig":invalid}`),
	} {
		lowered, changed := LowerGeminiNativeThinkingForSemanticRetry(input)
		require.False(t, changed)
		require.Equal(t, input, lowered)
	}
}
