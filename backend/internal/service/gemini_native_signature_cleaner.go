package service

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CleanGeminiNativeThoughtSignatures 从 Gemini 原生 API 请求中替换 thoughtSignature 字段为 dummy 签名，
// 以避免跨账号签名验证错误。
//
// 当粘性会话切换账号时（例如原账号异常、不可调度等），旧账号返回的 thoughtSignature
// 会导致新账号的签名验证失败。通过替换为 dummy 签名，跳过签名验证。
//
// CleanGeminiNativeThoughtSignatures replaces thoughtSignature fields with dummy signature
// in Gemini native API requests to avoid cross-account signature validation errors.
//
// When sticky session switches accounts (e.g., original account becomes unavailable),
// thoughtSignatures from the old account will cause validation failures on the new account.
// By replacing with dummy signature, we skip signature validation.
func CleanGeminiNativeThoughtSignatures(body []byte) []byte {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var data map[string]any
	if err := decoder.Decode(&data); err != nil {
		return body
	}
	changed := cleanGeminiThoughtSignatures(data)
	if !changed {
		return body
	}
	result, err := json.Marshal(data)
	if err != nil {
		return body
	}
	return result
}

// cleanGeminiThoughtSignatures only touches schema-defined Gemini part
// objects. User-controlled tool arguments may legally contain a key named
// thoughtSignature and must remain semantically unchanged.
func hasGeminiNativeThoughtSignature(body []byte) bool {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return false
	}
	hasPartSignature := func(parts gjson.Result) bool {
		found := false
		if parts.IsArray() {
			parts.ForEach(func(_, part gjson.Result) bool {
				if part.Get("thoughtSignature").Exists() {
					found = true
					return false
				}
				return true
			})
		}
		return found
	}
	contents := gjson.GetBytes(body, "contents")
	if contents.IsArray() {
		found := false
		contents.ForEach(func(_, content gjson.Result) bool {
			if hasPartSignature(content.Get("parts")) {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	if hasPartSignature(gjson.GetBytes(body, "cachedContent.parts")) {
		return true
	}
	request := gjson.GetBytes(body, "request")
	if request.IsObject() {
		if contents := request.Get("contents"); contents.IsArray() {
			found := false
			contents.ForEach(func(_, content gjson.Result) bool {
				if hasPartSignature(content.Get("parts")) {
					found = true
					return false
				}
				return true
			})
			if found {
				return true
			}
		}
		return hasPartSignature(request.Get("cachedContent.parts"))
	}
	return false
}

func cleanGeminiThoughtSignatures(data map[string]any) bool {
	changed := false
	if contents, ok := data["contents"].([]any); ok {
		for _, item := range contents {
			content, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if cleanGeminiPartList(content["parts"]) {
				changed = true
			}
		}
	}
	if cached, ok := data["cachedContent"].(map[string]any); ok {
		if cleanGeminiPartList(cached["parts"]) {
			changed = true
		}
	}
	if request, ok := data["request"].(map[string]any); ok {
		if cleanGeminiThoughtSignatures(request) {
			changed = true
		}
	}
	return changed
}

func cleanGeminiPartList(value any) bool {
	parts, ok := value.([]any)
	if !ok {
		return false
	}
	changed := false
	for _, item := range parts {
		part, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if _, exists := part["thoughtSignature"]; exists {
			part["thoughtSignature"] = antigravity.DummyThoughtSignature
			changed = true
		}
	}
	return changed
}

// LowerGeminiNativeThinkingForSemanticRetry lowers explicit thinking by one
// tier after an HTTP-successful response contained no usable model output.
// The retry ladder is HIGH -> MEDIUM -> LOW, so LOW is reached only after a
// MEDIUM retry also returns semantic-empty. Removing a stale numeric budget
// lets the selected wire tier supply its matching AGY budget (4000 or 1000).
// Initial requests remain unchanged; LOW and unsupported shapes are exact no-ops.
// Targeted gjson/sjson edits preserve unrelated numeric literals and tool args.
func LowerGeminiNativeThinkingForSemanticRetry(body []byte) ([]byte, bool) {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return body, false
	}
	bases := []string{
		"generationConfig.thinkingConfig",
		"config.thinkingConfig",
		"request.generationConfig.thinkingConfig",
		"request.config.thinkingConfig",
	}
	levelPath := ""
	basePath := ""
	var levelValue gjson.Result
	for _, base := range bases {
		for _, key := range []string{"thinkingLevel", "thinking_level"} {
			path := base + "." + key
			value := gjson.GetBytes(body, path)
			if value.Exists() && value.Type == gjson.String {
				basePath = base
				levelPath = path
				levelValue = value
				break
			}
		}
		if levelPath != "" {
			break
		}
	}
	if levelPath == "" {
		return body, false
	}
	var nextLevel string
	switch strings.ToUpper(strings.TrimSpace(levelValue.String())) {
	case "HIGH":
		nextLevel = "MEDIUM"
	case "MEDIUM":
		nextLevel = "LOW"
	default:
		return body, false
	}
	result, err := sjson.SetBytes(body, levelPath, nextLevel)
	if err != nil {
		return body, false
	}
	for _, key := range []string{"thinkingBudget", "thinking_budget"} {
		budgetPath := basePath + "." + key
		if gjson.GetBytes(result, budgetPath).Exists() {
			result, err = sjson.DeleteBytes(result, budgetPath)
			if err != nil {
				return body, false
			}
		}
	}
	return result, true
}
