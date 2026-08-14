package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigration177_ReplacesGemini37ModelMapping(t *testing.T) {
	content, err := FS.ReadFile("177_replace_gemini37_model_mapping.sql")
	require.NoError(t, err)

	sql := string(content)

	// Asserts three exact identity mappings are added
	require.Contains(t, sql, `"gemini-3.7-flash-high": "gemini-3.7-flash-high"`)
	require.Contains(t, sql, `"gemini-3.7-flash-medium": "gemini-3.7-flash-medium"`)
	require.Contains(t, sql, `"gemini-3.7-flash-low": "gemini-3.7-flash-low"`)

	// Asserts conditional removal of erroneous suffixless identity mapping
	require.Contains(t, sql, "a.credentials->'model_mapping'->>'gemini-3.7-flash' = 'gemini-3.7-flash'")
	require.Contains(t, sql, "- 'gemini-3.7-flash'")

	// Asserts platforms targeted
	require.Contains(t, sql, "a.platform IN ('antigravity', 'antigravity_native')")
}
