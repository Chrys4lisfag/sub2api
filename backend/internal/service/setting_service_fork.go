package service

// Fork additions to SettingService, re-applied after the upstream merge that
// split the monolithic setting_service.go into setting_*.go files. These are
// the config getters + enums for our fork features: warp-panel proxy farm,
// browser2webfront login stream, antigravity_native MCP discovery/tool-call
// modes + aggregator name, and chat-history logging. The cache-holder struct
// fields live on the SettingService struct in setting_service.go.

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// cachedAntigravityNativeMcpDiscoveryMode — 60s TTL cache for the
// global MCP discovery mode enum. Read on every native gateway request.
type cachedAntigravityNativeMcpDiscoveryMode struct {
	value     string // "prompt" | "list_tool" | "both"
	expiresAt int64
}

const antigravityNativeMcpDiscoveryModeCacheTTL = 60 * time.Second
const antigravityNativeMcpDiscoveryModeDBTimeout = 5 * time.Second

// cachedAntigravityNativeToolCallMode — 60s TTL cache for the
// global MCP tool-call mode enum.
type cachedAntigravityNativeToolCallMode struct {
	value     string // "single_name" | "agy_mimic"
	expiresAt int64
}

const antigravityNativeToolCallModeCacheTTL = 60 * time.Second
const antigravityNativeToolCallModeDBTimeout = 5 * time.Second

// cachedChatHistoryEnabled — 60s TTL cache for the chat-history global toggle.
type cachedChatHistoryEnabled struct {
	value     bool
	expiresAt int64
}

// cachedChatHistoryMaxBytes — 60s TTL cache for the size cap.
type cachedChatHistoryMaxBytes struct {
	value     int64
	expiresAt int64
}

const chatHistoryCacheTTL = 60 * time.Second
const chatHistoryErrorTTL = 5 * time.Second
const chatHistoryDBTimeout = 5 * time.Second

// chatHistoryDefaultMaxBytes is the default cap: 500 MiB total on-disk.
const chatHistoryDefaultMaxBytes int64 = 524288000

// cachedAntigravityNativeMcpAggregatorName — 60s TTL cache for the
// global default MCP aggregator function name.
type cachedAntigravityNativeMcpAggregatorName struct {
	value     string
	expiresAt int64
}

const antigravityNativeMcpAggregatorNameCacheTTL = 60 * time.Second
const antigravityNativeMcpAggregatorNameErrorTTL = 5 * time.Second
const antigravityNativeMcpAggregatorNameDBTimeout = 5 * time.Second

// GetWarpPanelConfig 读取 warp-panel 集成配置（base URL + Basic Auth）。
// 三个值均取自 `settings` 表；缺失时返回空串，调用方负责给出人类可读错误。
func (s *SettingService) GetWarpPanelConfig(ctx context.Context) (baseURL, user, pass string) {
	values, err := s.settingRepo.GetMultiple(ctx, []string{
		SettingKeyWarpPanelURL,
		SettingKeyWarpPanelUser,
		SettingKeyWarpPanelPass,
	})
	if err != nil {
		return "", "", ""
	}
	return strings.TrimSpace(values[SettingKeyWarpPanelURL]),
		strings.TrimSpace(values[SettingKeyWarpPanelUser]),
		strings.TrimSpace(values[SettingKeyWarpPanelPass])
}

// GetBrowserLoginConfig 读取 browser2webfront 集成配置（base URL + Basic Auth）。
func (s *SettingService) GetBrowserLoginConfig(ctx context.Context) (baseURL, user, pass string) {
	values, err := s.settingRepo.GetMultiple(ctx, []string{
		SettingKeyBrowserLoginURL,
		SettingKeyBrowserLoginUser,
		SettingKeyBrowserLoginPass,
	})
	if err != nil {
		return "", "", ""
	}
	return strings.TrimSpace(values[SettingKeyBrowserLoginURL]),
		strings.TrimSpace(values[SettingKeyBrowserLoginUser]),
		strings.TrimSpace(values[SettingKeyBrowserLoginPass])
}

// GetBrowserLoginVNCURL returns the browser-reachable noVNC public origin.
func (s *SettingService) GetBrowserLoginVNCURL(ctx context.Context) string {
	values, err := s.settingRepo.GetMultiple(ctx, []string{SettingKeyBrowserLoginVNCURL})
	if err != nil {
		return ""
	}
	return strings.TrimRight(strings.TrimSpace(values[SettingKeyBrowserLoginVNCURL]), "/")
}

// Mcp discovery mode constants.
const (
	McpDiscoveryModePrompt   = "prompt"
	McpDiscoveryModeListTool = "list_tool"
	McpDiscoveryModeBoth     = "both"
)

// GetAntigravityNativeMcpDiscoveryMode returns the effective discovery mode
// (new key → legacy bool key migration → "both" default). 60s TTL cache.
func (s *SettingService) GetAntigravityNativeMcpDiscoveryMode(ctx context.Context) string {
	const defaultMode = McpDiscoveryModeBoth
	if cached, ok := s.antigravityNativeMcpDiscoveryModeCache.Load().(*cachedAntigravityNativeMcpDiscoveryMode); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := s.antigravityNativeMcpDiscoveryModeSF.Do("antigravity_native_mcp_discovery_mode", func() (any, error) {
		if cached, ok := s.antigravityNativeMcpDiscoveryModeCache.Load().(*cachedAntigravityNativeMcpDiscoveryMode); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), antigravityNativeMcpDiscoveryModeDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyAntigravityNativeMcpDiscoveryMode)
		mode := normalizeMcpDiscoveryMode(value)
		if err == nil && mode != "" {
			s.antigravityNativeMcpDiscoveryModeCache.Store(&cachedAntigravityNativeMcpDiscoveryMode{
				value:     mode,
				expiresAt: time.Now().Add(antigravityNativeMcpDiscoveryModeCacheTTL).UnixNano(),
			})
			return mode, nil
		}
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			slog.Warn("failed to get antigravity_native_mcp_discovery_mode setting", "error", err)
		}
		legacyVal, legacyErr := s.settingRepo.GetValue(dbCtx, SettingKeyAntigravityNativeListToolsEmulation)
		legacyMode := defaultMode
		if legacyErr == nil {
			switch legacyVal {
			case "true":
				legacyMode = McpDiscoveryModeBoth
			case "false":
				legacyMode = McpDiscoveryModePrompt
			}
		}
		s.antigravityNativeMcpDiscoveryModeCache.Store(&cachedAntigravityNativeMcpDiscoveryMode{
			value:     legacyMode,
			expiresAt: time.Now().Add(antigravityNativeMcpDiscoveryModeCacheTTL).UnixNano(),
		})
		return legacyMode, nil
	})
	if val, ok := result.(string); ok && val != "" {
		return val
	}
	return defaultMode
}

// normalizeMcpDiscoveryMode validates + canonicalizes the enum value.
func normalizeMcpDiscoveryMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case McpDiscoveryModePrompt, "prompt_only", "prompt-only":
		return McpDiscoveryModePrompt
	case McpDiscoveryModeListTool, "list_tool_only", "list-tool", "list-tool-only":
		return McpDiscoveryModeListTool
	case McpDiscoveryModeBoth, "all", "full":
		return McpDiscoveryModeBoth
	}
	return ""
}

// Tool call mode constants.
const (
	ToolCallModeSingleName = "single_name"
	ToolCallModeAgyMimic   = "agy_mimic"
)

// GetAntigravityNativeToolCallMode returns the effective tool-call mode
// (default "single_name"). 60s TTL cache.
func (s *SettingService) GetAntigravityNativeToolCallMode(ctx context.Context) string {
	const defaultMode = ToolCallModeSingleName
	if cached, ok := s.antigravityNativeToolCallModeCache.Load().(*cachedAntigravityNativeToolCallMode); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := s.antigravityNativeToolCallModeSF.Do("antigravity_native_tool_call_mode", func() (any, error) {
		if cached, ok := s.antigravityNativeToolCallModeCache.Load().(*cachedAntigravityNativeToolCallMode); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), antigravityNativeToolCallModeDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyAntigravityNativeToolCallMode)
		mode := normalizeToolCallMode(value)
		if err == nil && mode != "" {
			s.antigravityNativeToolCallModeCache.Store(&cachedAntigravityNativeToolCallMode{
				value:     mode,
				expiresAt: time.Now().Add(antigravityNativeToolCallModeCacheTTL).UnixNano(),
			})
			return mode, nil
		}
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			slog.Warn("failed to get antigravity_native_tool_call_mode setting", "error", err)
		}
		s.antigravityNativeToolCallModeCache.Store(&cachedAntigravityNativeToolCallMode{
			value:     defaultMode,
			expiresAt: time.Now().Add(antigravityNativeToolCallModeCacheTTL).UnixNano(),
		})
		return defaultMode, nil
	})
	if val, ok := result.(string); ok && val != "" {
		return val
	}
	return defaultMode
}

// normalizeToolCallMode validates + canonicalizes the enum value.
func normalizeToolCallMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case ToolCallModeSingleName, "single-name", "passthrough", "direct":
		return ToolCallModeSingleName
	case ToolCallModeAgyMimic, "agy-mimic", "agy", "mimic", "aggregator":
		return ToolCallModeAgyMimic
	}
	return ""
}

// IsChatHistoryEnabled returns the global chat-history toggle (default true).
func (s *SettingService) IsChatHistoryEnabled(ctx context.Context) bool {
	if cached, ok := s.chatHistoryEnabledCache.Load().(*cachedChatHistoryEnabled); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := s.chatHistoryEnabledSF.Do("chat_history_enabled", func() (any, error) {
		if cached, ok := s.chatHistoryEnabledCache.Load().(*cachedChatHistoryEnabled); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chatHistoryDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyChatHistoryEnabled)
		if err != nil {
			if errors.Is(err, ErrSettingNotFound) {
				s.chatHistoryEnabledCache.Store(&cachedChatHistoryEnabled{
					value:     true,
					expiresAt: time.Now().Add(chatHistoryCacheTTL).UnixNano(),
				})
				return true, nil
			}
			slog.Warn("failed to get chat_history_enabled setting", "error", err)
			s.chatHistoryEnabledCache.Store(&cachedChatHistoryEnabled{
				value:     true,
				expiresAt: time.Now().Add(chatHistoryErrorTTL).UnixNano(),
			})
			return true, nil
		}
		v := strings.TrimSpace(value)
		enabled := v == "" || v == "true"
		s.chatHistoryEnabledCache.Store(&cachedChatHistoryEnabled{
			value:     enabled,
			expiresAt: time.Now().Add(chatHistoryCacheTTL).UnixNano(),
		})
		return enabled, nil
	})
	if val, ok := result.(bool); ok {
		return val
	}
	return true
}

// GetChatHistoryMaxBytes returns the global cap (default 500 MiB). 60s TTL.
func (s *SettingService) GetChatHistoryMaxBytes(ctx context.Context) int64 {
	if cached, ok := s.chatHistoryMaxBytesCache.Load().(*cachedChatHistoryMaxBytes); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := s.chatHistoryMaxBytesSF.Do("chat_history_max_bytes", func() (any, error) {
		if cached, ok := s.chatHistoryMaxBytesCache.Load().(*cachedChatHistoryMaxBytes); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chatHistoryDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyChatHistoryMaxBytes)
		if err != nil {
			if errors.Is(err, ErrSettingNotFound) {
				s.chatHistoryMaxBytesCache.Store(&cachedChatHistoryMaxBytes{
					value:     chatHistoryDefaultMaxBytes,
					expiresAt: time.Now().Add(chatHistoryCacheTTL).UnixNano(),
				})
				return chatHistoryDefaultMaxBytes, nil
			}
			slog.Warn("failed to get chat_history_max_bytes setting", "error", err)
			s.chatHistoryMaxBytesCache.Store(&cachedChatHistoryMaxBytes{
				value:     chatHistoryDefaultMaxBytes,
				expiresAt: time.Now().Add(chatHistoryErrorTTL).UnixNano(),
			})
			return chatHistoryDefaultMaxBytes, nil
		}
		parsed, perr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if perr != nil || parsed <= 0 {
			parsed = chatHistoryDefaultMaxBytes
		}
		s.chatHistoryMaxBytesCache.Store(&cachedChatHistoryMaxBytes{
			value:     parsed,
			expiresAt: time.Now().Add(chatHistoryCacheTTL).UnixNano(),
		})
		return parsed, nil
	})
	if val, ok := result.(int64); ok && val > 0 {
		return val
	}
	return chatHistoryDefaultMaxBytes
}

// GetAntigravityNativeMcpAggregatorName — 全局默认 MCP 聚合器函数名（默认 "call_mcp_tool"）。
func (s *SettingService) GetAntigravityNativeMcpAggregatorName(ctx context.Context) string {
	const fallback = "call_mcp_tool"
	if cached, ok := s.antigravityNativeMcpAggregatorNameCache.Load().(*cachedAntigravityNativeMcpAggregatorName); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	result, _, _ := s.antigravityNativeMcpAggregatorNameSF.Do("antigravity_native_mcp_aggregator_name", func() (any, error) {
		if cached, ok := s.antigravityNativeMcpAggregatorNameCache.Load().(*cachedAntigravityNativeMcpAggregatorName); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), antigravityNativeMcpAggregatorNameDBTimeout)
		defer cancel()
		value, err := s.settingRepo.GetValue(dbCtx, SettingKeyAntigravityNativeMcpAggregatorName)
		if err != nil {
			if errors.Is(err, ErrSettingNotFound) {
				s.antigravityNativeMcpAggregatorNameCache.Store(&cachedAntigravityNativeMcpAggregatorName{
					value:     fallback,
					expiresAt: time.Now().Add(antigravityNativeMcpAggregatorNameCacheTTL).UnixNano(),
				})
				return fallback, nil
			}
			slog.Warn("failed to get antigravity_native_mcp_aggregator_name setting", "error", err)
			s.antigravityNativeMcpAggregatorNameCache.Store(&cachedAntigravityNativeMcpAggregatorName{
				value:     fallback,
				expiresAt: time.Now().Add(antigravityNativeMcpAggregatorNameErrorTTL).UnixNano(),
			})
			return fallback, nil
		}
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || !isValidMcpAggregatorName(trimmed) {
			trimmed = fallback
		}
		s.antigravityNativeMcpAggregatorNameCache.Store(&cachedAntigravityNativeMcpAggregatorName{
			value:     trimmed,
			expiresAt: time.Now().Add(antigravityNativeMcpAggregatorNameCacheTTL).UnixNano(),
		})
		return trimmed, nil
	})
	if val, ok := result.(string); ok && val != "" {
		return val
	}
	return fallback
}
