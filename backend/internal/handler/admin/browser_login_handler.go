// BrowserLoginHandler exposes admin endpoints that drive the standalone
// browser2webfront stealth-browser stream so an admin can perform an
// interactive OAuth login inside a streamed browser.
//
// Endpoints (registered in routes/admin.go):
//
//	POST   /api/v1/admin/browser-login/session          -> start the single session
//	POST   /api/v1/admin/browser-login/navigate         -> drive the tab to a URL
//	GET    /api/v1/admin/browser-login/result           -> captured callback code + url
//	POST   /api/v1/admin/browser-login/google-autologin -> start Google activation
//	GET    /api/v1/admin/browser-login/google-autologin -> read activation status
//	DELETE /api/v1/admin/browser-login/google-autologin -> cancel Google activation
//	DELETE /api/v1/admin/browser-login/session          -> tear the session down
//
// Upstream (browser2webfront) errors preserve their HTTP status while exposing
// only the service's bounded, sanitized message.
package admin

import (
	"errors"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type BrowserLoginHandler struct {
	svc *service.BrowserLoginService
}

func NewBrowserLoginHandler(svc *service.BrowserLoginService) *BrowserLoginHandler {
	return &BrowserLoginHandler{svc: svc}
}

func writeBrowserLoginError(c *gin.Context, err error) {
	var upstreamErr *service.BrowserLoginUpstreamError
	if errors.As(err, &upstreamErr) {
		response.Error(c, upstreamErr.StatusCode, upstreamErr.Error())
		return
	}
	response.BadRequest(c, err.Error())
}

type BrowserLoginStartRequest struct {
	ProxyID   *int64 `json:"proxy_id"`
	ProfileID string `json:"profile_id"`
}

type GoogleAutologinRequest struct {
	AccountID           *int64 `json:"account_id"`
	Login               string `json:"login" binding:"required"`
	Password            string `json:"password" binding:"required"`
	TwoFactorImportCode string `json:"two_factor_import_code"`
}

// StartSession — POST /api/v1/admin/browser-login/session
func (h *BrowserLoginHandler) StartSession(c *gin.Context) {
	var req BrowserLoginStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效: "+err.Error())
		return
	}
	sess, err := h.svc.StartSession(c.Request.Context(), &service.BrowserLoginStartInput{
		ProxyID:   req.ProxyID,
		ProfileID: req.ProfileID,
	})
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, sess)
}

type BrowserLoginNavigateRequest struct {
	URL string `json:"url" binding:"required"`
}

// Navigate — POST /api/v1/admin/browser-login/navigate
func (h *BrowserLoginHandler) Navigate(c *gin.Context) {
	var req BrowserLoginNavigateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效: "+err.Error())
		return
	}
	res, err := h.svc.Navigate(c.Request.Context(), strings.TrimSpace(c.GetHeader("X-Browser-Session-ID")), req.URL)
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, res)
}

// Result — GET /api/v1/admin/browser-login/result
func (h *BrowserLoginHandler) Result(c *gin.Context) {
	res, err := h.svc.Result(c.Request.Context(), strings.TrimSpace(c.GetHeader("X-Browser-Session-ID")))
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, res)
}

// RunGoogleAutologin — POST /api/v1/admin/browser-login/google-autologin
func (h *BrowserLoginHandler) RunGoogleAutologin(c *gin.Context) {
	sessionID := strings.TrimSpace(c.GetHeader("X-Browser-Session-ID"))
	if sessionID == "" {
		response.BadRequest(c, "X-Browser-Session-ID header is required")
		return
	}
	var req GoogleAutologinRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "请求无效")
		return
	}
	status, err := h.svc.RunGoogleAutologin(c.Request.Context(), sessionID, &service.GoogleAutologinInput{
		AccountID:           req.AccountID,
		Login:               req.Login,
		Password:            req.Password,
		TwoFactorImportCode: req.TwoFactorImportCode,
	})
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, status)
}

// GetGoogleAutologinStatus — GET /api/v1/admin/browser-login/google-autologin
func (h *BrowserLoginHandler) GetGoogleAutologinStatus(c *gin.Context) {
	sessionID := strings.TrimSpace(c.GetHeader("X-Browser-Session-ID"))
	if sessionID == "" {
		response.BadRequest(c, "X-Browser-Session-ID header is required")
		return
	}
	status, err := h.svc.GoogleAutologinStatus(c.Request.Context(), sessionID)
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, status)
}

// CancelGoogleAutologin — DELETE /api/v1/admin/browser-login/google-autologin
func (h *BrowserLoginHandler) CancelGoogleAutologin(c *gin.Context) {
	sessionID := strings.TrimSpace(c.GetHeader("X-Browser-Session-ID"))
	if sessionID == "" {
		response.BadRequest(c, "X-Browser-Session-ID header is required")
		return
	}
	status, err := h.svc.CancelGoogleAutologin(c.Request.Context(), sessionID)
	if err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, status)
}

// StopSession — DELETE /api/v1/admin/browser-login/session
func (h *BrowserLoginHandler) StopSession(c *gin.Context) {
	if err := h.svc.StopSession(c.Request.Context(), strings.TrimSpace(c.GetHeader("X-Browser-Session-ID"))); err != nil {
		writeBrowserLoginError(c, err)
		return
	}
	response.Success(c, gin.H{"ok": true})
}
