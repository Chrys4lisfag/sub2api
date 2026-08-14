package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultModelsListCandidateIDsAntigravityNative(t *testing.T) {
	models := defaultModelsListCandidateIDs(PlatformAntigravityNative)

	require.Contains(t, models, "gemini-3.1-flash-lite")
	require.Contains(t, models, "gemini-3.6-flash")
	require.Contains(t, models, "gemini-3.6-flash-high")
	require.Contains(t, models, "gemini-3.6-flash-medium")
	require.Contains(t, models, "gemini-3.6-flash-low")
	require.Contains(t, models, "gemini-3.7-flash")
}
