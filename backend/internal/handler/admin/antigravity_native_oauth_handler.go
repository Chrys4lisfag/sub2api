// AntigravityNativeOAuthHandler exposes admin endpoints for the
// PlatformAntigravityNative OAuth flow.
//
// Endpoints (registered in routes/admin.go):
//
//	POST /api/v1/admin/antigravity-native/oauth/auth-url
//	POST /api/v1/admin/antigravity-native/oauth/exchange-code
//	POST /api/v1/admin/antigravity-native/oauth/refresh-token
//
// DTO shapes mirror AntigravityOAuthHandler so the frontend can reuse
// most of its existing form components — only the API path differs.
package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type AntigravityNativeOAuthHandler struct {
	svc *service.AntigravityNativeOAuthService
}

func NewAntigravityNativeOAuthHandler(svc *service.AntigravityNativeOAuthService) *AntigravityNativeOAuthHandler {
	return &AntigravityNativeOAuthHandler{svc: svc}
}

type AntigravityNativeGenerateAuthURLRequest struct {
	ProxyID *int64 `json:"proxy_id"`
}

// GenerateAuthURL — POST /api/v1/admin/antigravity-native/oauth/auth-url
func (h *AntigravityNativeOAuthHandler) GenerateAuthURL(c *gin.Context) {
	var req AntigravityNativeGenerateAuthURLRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效: "+err.Error())
		return
	}
	result, err := h.svc.GenerateAuthURL(c.Request.Context(), req.ProxyID)
	if err != nil {
		response.InternalError(c, "生成授权链接失败: "+err.Error())
		return
	}
	response.Success(c, result)
}

type AntigravityNativeExchangeCodeRequest struct {
	SessionID string `json:"session_id" binding:"required"`
	State     string `json:"state" binding:"required"`
	Code      string `json:"code" binding:"required"`
	ProxyID   *int64 `json:"proxy_id"`
}

// ExchangeCode — POST /api/v1/admin/antigravity-native/oauth/exchange-code
func (h *AntigravityNativeOAuthHandler) ExchangeCode(c *gin.Context) {
	var req AntigravityNativeExchangeCodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效: "+err.Error())
		return
	}
	tokenInfo, err := h.svc.ExchangeCode(c.Request.Context(), &service.AntigravityNativeExchangeCodeInput{
		SessionID: req.SessionID,
		State:     req.State,
		Code:      req.Code,
		ProxyID:   req.ProxyID,
	})
	if err != nil {
		// Pass the verbatim upstream error to the UI — agymimic now
		// returns the raw Google OAuth error body or the underlying
		// transport failure (proxy refused, TLS, DNS). The operator
		// already knows they're at the exchange step from the dialog
		// context; no human-framing prefix needed.
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, tokenInfo)
}

type AntigravityNativeRefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
	ProxyID      *int64 `json:"proxy_id"`
}

// RefreshToken — POST /api/v1/admin/antigravity-native/oauth/refresh-token
func (h *AntigravityNativeOAuthHandler) RefreshToken(c *gin.Context) {
	var req AntigravityNativeRefreshTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效: "+err.Error())
		return
	}
	tokenInfo, err := h.svc.ValidateRefreshToken(c.Request.Context(), req.RefreshToken, req.ProxyID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, tokenInfo)
}
