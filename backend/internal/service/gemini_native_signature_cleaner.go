package service

import (
	"encoding/json"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
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
	if len(body) == 0 {
		return body
	}

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
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
// thoughtSignature and must remain byte-for-byte equivalent.
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
