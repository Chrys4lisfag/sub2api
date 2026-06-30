package service

import (
	"context"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
)

func normalizeAntigravityModelName(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndex(normalized, "/publishers/google/models/"); idx != -1 {
		normalized = normalized[idx+len("/publishers/google/models/"):]
	} else if idx := strings.LastIndex(normalized, "/publishers/anthropic/models/"); idx != -1 {
		normalized = normalized[idx+len("/publishers/anthropic/models/"):]
	} else if idx := strings.LastIndex(normalized, "/models/"); idx != -1 {
		normalized = normalized[idx+len("/models/"):]
	} else {
		normalized = strings.TrimPrefix(normalized, "publishers/google/models/")
		normalized = strings.TrimPrefix(normalized, "publishers/anthropic/models/")
		normalized = strings.TrimPrefix(normalized, "models/")
	}
	return normalized
}

// resolveAntigravityModelKey 根据请求的模型名解析限流 key
// 返回空字符串表示无法解析
func resolveAntigravityModelKey(requestedModel string) string {
	return normalizeAntigravityModelName(requestedModel)
}

// IsSchedulableForModel 结合模型级限流判断是否可调度。
// 保持旧签名以兼容既有调用方；默认使用 context.Background()。
func (a *Account) IsSchedulableForModel(requestedModel string) bool {
	return a.IsSchedulableForModelWithContext(context.Background(), requestedModel)
}

func (a *Account) IsSchedulableForModelWithContext(ctx context.Context, requestedModel string) bool {
	if a == nil {
		return false
	}
	if !a.IsSchedulable() {
		return false
	}
	// Native antigravity: gate on the dashboard's USAGE WINDOWS data
	// (account.Extra["antigravity_quota"], written by AccountUsageService.
	// persistAntigravityQuotaToExtra on every successful quota fetch).
	// No separate model_rate_limits write needed — same signal that
	// drives the red bars in the admin UI also drives selection here.
	if a.Platform == PlatformAntigravityNative && a.isAntigravityNativeModelExhausted(requestedModel) {
		return false
	}
	if a.isModelRateLimitedWithContext(ctx, requestedModel) {
		// Antigravity + overages 启用 + 积分未耗尽 → 放行（有积分可用）
		if a.Platform == PlatformAntigravity && a.IsOveragesEnabled() && !a.isCreditsExhausted() {
			return true
		}
		return false
	}
	return true
}

// isAntigravityNativeModelExhausted consults the persisted USAGE
// WINDOWS snapshot (account.Extra["antigravity_quota"]) and reports
// whether the requested model's tier is at 100 % utilization with the
// reset window still in the future. Returns false on every "we don't
// know" condition (missing snapshot, parse error, expired reset) so a
// missing signal NEVER over-blocks scheduling — the gateway's 429
// failover still catches the case if a request gets through.
//
// The snapshot is keyed by the upstream's model names (e.g.
// gemini-3-flash, gemini-3.1-flash-image). When the requested model
// matches one of those keys directly, we use the exact entry.
// Otherwise we also check the wire-translated form via
// antigravity.AntigravityWireModel so a request for
// `gemini-3.5-flash-high` (public) finds the snapshot entry under
// `gemini-3-flash-agent` (wire) when upstream keys it that way.
func (a *Account) isAntigravityNativeModelExhausted(requestedModel string) bool {
	if a == nil || len(a.Extra) == 0 {
		return false
	}
	rawQuota, ok := a.Extra["antigravity_quota"]
	if !ok {
		return false
	}
	quota, ok := rawQuota.(map[string]any)
	if !ok || len(quota) == 0 {
		return false
	}

	requested := normalizeAntigravityModelName(requestedModel)
	candidates := []string{requested}
	if wire := antigravity.AntigravityWireModel(requested); wire != "" && wire != requested {
		candidates = append(candidates, wire)
	}

	now := time.Now()
	for _, key := range candidates {
		entry, ok := quota[key].(map[string]any)
		if !ok {
			continue
		}
		util, _ := entry["utilization"].(float64)
		if util < 100 {
			continue
		}
		// Reset already passed → snapshot stale, don't over-block. The
		// next dashboard refresh will rewrite the snapshot.
		if resetStr, ok := entry["reset_time"].(string); ok && resetStr != "" {
			if reset, err := time.Parse(time.RFC3339, resetStr); err == nil && !now.Before(reset) {
				continue
			}
		}
		return true
	}
	return false
}

// GetRateLimitRemainingTime 获取限流剩余时间（模型级限流）
// 返回 0 表示未限流或已过期
func (a *Account) GetRateLimitRemainingTime(requestedModel string) time.Duration {
	return a.GetRateLimitRemainingTimeWithContext(context.Background(), requestedModel)
}

// GetRateLimitRemainingTimeWithContext 获取限流剩余时间（模型级限流）
// 返回 0 表示未限流或已过期
func (a *Account) GetRateLimitRemainingTimeWithContext(ctx context.Context, requestedModel string) time.Duration {
	if a == nil {
		return 0
	}
	return a.GetModelRateLimitRemainingTimeWithContext(ctx, requestedModel)
}
