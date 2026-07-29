package service

import (
	"context"
	"log/slog"

	"github.com/Wei-Shaw/sub2api/internal/pkg/diagnosticcapture"
	"github.com/gin-gonic/gin"
)

const (
	antigravityABPayloadByteLimit  = 1 << 20
	antigravityABPayloadCountLimit = 256
)

func captureAntigravityAB(ctx context.Context, c *gin.Context, record diagnosticcapture.Record) {
	if !diagnosticcapture.Enabled() {
		return
	}
	if c != nil && c.Request != nil && c.Request.URL != nil {
		record.RequestPath = c.Request.URL.Path
	}
	if err := diagnosticcapture.Capture(ctx, record); err != nil {
		slog.WarnContext(ctx, "antigravity A/B capture failed", "error", err.Error())
	}
}
