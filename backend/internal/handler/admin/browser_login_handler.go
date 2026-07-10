// BrowserLoginHandler exposes admin endpoints that drive the standalone
// browser2webfront stealth-browser stream so an admin can perform an
// interactive OAuth login inside a streamed browser.
//
// Endpoints (registered in routes/admin.go):
//
//	POST   /api/v1/admin/browser-login/session   -> start the single session
//	POST   /api/v1/admin/browser-login/navigate  -> drive the tab to a URL
//	GET    /api/v1/admin/browser-login/result     -> captured callback code + url
//	DELETE /api/v1/admin/browser-login/session    -> tear the session down
//
// Upstream (browser2webfront) errors — including a 409 "session busy" or a
// not-configured message — are passed through verbatim so the admin sees the
// real cause in the dialog.
package admin

import (
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

type BrowserLoginStartRequest struct {
	ProxyID   *int64 `json:"proxy_id"`
	ProfileID string `json:"profile_id"`
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
		response.BadRequest(c, err.Error())
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
	if err := h.svc.Navigate(c.Request.Context(), req.URL); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"ok": true})
}

// Result — GET /api/v1/admin/browser-login/result
func (h *BrowserLoginHandler) Result(c *gin.Context) {
	res, err := h.svc.Result(c.Request.Context())
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, res)
}

// StopSession — DELETE /api/v1/admin/browser-login/session
func (h *BrowserLoginHandler) StopSession(c *gin.Context) {
	if err := h.svc.StopSession(c.Request.Context()); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"ok": true})
}
