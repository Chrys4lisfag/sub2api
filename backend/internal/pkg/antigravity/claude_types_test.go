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
	}

	for _, id := range requiredIDs {
		if _, ok := byID[id]; !ok {
			t.Fatalf("expected model %q to be exposed in DefaultModels", id)
		}
	}
}

func TestDefaultGeminiModels_ContainsGemini37Flash(t *testing.T) {
	t.Parallel()

	geminiModels := DefaultGeminiModels()
	var found *GeminiModel
	for i := range geminiModels {
		if geminiModels[i].Name == "models/gemini-3.7-flash" {
			found = &geminiModels[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected models/gemini-3.7-flash in DefaultGeminiModels")
	}
	if found.DisplayName != "Gemini 3.7 Flash" {
		t.Fatalf("unexpected display name: got %q want %q", found.DisplayName, "Gemini 3.7 Flash")
	}
}
