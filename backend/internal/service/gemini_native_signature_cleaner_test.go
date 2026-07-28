package service

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/stretchr/testify/require"
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

func TestCleanGeminiNativeThoughtSignatures_ReplacesEscapedPartKey(t *testing.T) {
	input := []byte(`{"contents":[{"parts":[{"\u0074houghtSignature":"stale"}]}]}`)
	cleaned := CleanGeminiNativeThoughtSignatures(input)
	require.Contains(t, string(cleaned), `"thoughtSignature":"`+antigravity.DummyThoughtSignature+`"`)
}

func TestCleanGeminiNativeThoughtSignatures_NoSchemaSignatureReturnsOriginal(t *testing.T) {
	input := []byte(`{"functionCall":{"args":{"thoughtSignature":"user-data"}}}`)
	require.Equal(t, input, CleanGeminiNativeThoughtSignatures(input))
}
