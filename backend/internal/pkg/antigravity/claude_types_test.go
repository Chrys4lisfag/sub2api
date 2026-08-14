package antigravity

import "testing"

func TestDefaultModels_ContainsNewAndLegacyImageModels(t *testing.T) {
	t.Parallel()

	models := DefaultModels()
	byID := make(map[string]ClaudeModel, len(models))
	for _, m := range models {
		byID[m.ID] = m
	}

	requiredIDs := []string{
		"claude-fable-5",
		"claude-opus-4-8",
		"claude-opus-4-6-thinking",
		"gemini-2.5-flash-image",
		"gemini-2.5-flash-image-preview",
		"gemini-3.1-flash-image",
		"gemini-3.1-flash-image-preview",
		"gemini-3.7-flash-high",
		"gemini-3.7-flash-medium",
		"gemini-3.7-flash-low",
	}

	for _, id := range requiredIDs {
		if _, ok := byID[id]; !ok {
			t.Fatalf("expected model %q to be exposed in DefaultModels", id)
		}
	}

	if _, ok := byID["gemini-3.7-flash"]; ok {
		t.Fatalf("did not expect suffixless gemini-3.7-flash in DefaultModels")
	}
}

func TestDefaultGeminiModels_ContainsGemini37FlashTiers(t *testing.T) {
	t.Parallel()

	geminiModels := DefaultGeminiModels()
	byName := make(map[string]GeminiModel, len(geminiModels))
	for _, m := range geminiModels {
		byName[m.Name] = m
	}

	expected := map[string]string{
		"models/gemini-3.7-flash-high":   "Gemini 3.7 Flash (High)",
		"models/gemini-3.7-flash-medium": "Gemini 3.7 Flash (Medium)",
		"models/gemini-3.7-flash-low":    "Gemini 3.7 Flash (Low)",
	}

	for name, wantDisplay := range expected {
		m, ok := byName[name]
		if !ok {
			t.Fatalf("expected %q in DefaultGeminiModels", name)
		}
		if m.DisplayName != wantDisplay {
			t.Fatalf("unexpected display name for %q: got %q want %q", name, m.DisplayName, wantDisplay)
		}
	}

	if _, ok := byName["models/gemini-3.7-flash"]; ok {
		t.Fatalf("did not expect models/gemini-3.7-flash in DefaultGeminiModels")
	}
}
