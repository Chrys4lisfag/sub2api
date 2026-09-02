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
		"gemini-3.7-flash",
		"gemini-3.8-flash",
	}

	for _, id := range requiredIDs {
		if _, ok := byID[id]; !ok {
			t.Fatalf("expected model %q to be exposed in DefaultModels", id)
		}
	}

	for _, tier := range []string{
		"gemini-3.7-flash-high", "gemini-3.7-flash-medium", "gemini-3.7-flash-low",
		"gemini-3.8-flash-high", "gemini-3.8-flash-medium", "gemini-3.8-flash-low",
	} {
		if _, ok := byID[tier]; ok {
			t.Fatalf("did not expect internal tier %q in DefaultModels", tier)
		}
	}
}

func TestDefaultGeminiModels_ContainsGemini37FlashVirtualAlias(t *testing.T) {
	t.Parallel()

	geminiModels := DefaultGeminiModels()
	byName := make(map[string]GeminiModel, len(geminiModels))
	for _, m := range geminiModels {
		byName[m.Name] = m
	}

	m, ok := byName["models/gemini-3.7-flash"]
	if !ok {
		t.Fatal("expected virtual Gemini 3.7 Flash alias in DefaultGeminiModels")
	}
	if m.DisplayName != "Gemini 3.7 Flash" {
		t.Fatalf("unexpected display name: %q", m.DisplayName)
	}
	for _, tier := range []string{"high", "medium", "low"} {
		if _, ok := byName["models/gemini-3.7-flash-"+tier]; ok {
			t.Fatalf("did not expect internal tier %q in DefaultGeminiModels", tier)
		}
	}
}
