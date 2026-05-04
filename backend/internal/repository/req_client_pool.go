package repository

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/proxyurl"

	"github.com/imroc/req/v3"
)

// reqClientOptions 定义 req 客户端的构建参数
type reqClientOptions struct {
	ProxyURL    string        // 代理 URL（支持 http/https/socks5）
	Timeout     time.Duration // 请求超时时间
	Impersonate bool          // 是否模拟 Chrome 浏览器指纹
	ForceHTTP2  bool          // 是否强制使用 HTTP/2
}

// sharedReqClients 存储按配置参数缓存的 req 客户端实例
//
// 性能优化说明：
// 原实现在每次 OAuth 刷新时都创建新的 req.Client：
// 1. claude_oauth_service.go: 每次刷新创建新客户端
// 2. openai_oauth_service.go: 每次刷新创建新客户端
// 3. gemini_oauth_client.go: 每次刷新创建新客户端
//
// 新实现使用 sync.Map 缓存客户端：
// 1. 相同配置（代理+超时+模拟设置）复用同一客户端
// 2. 复用底层连接池，减少 TLS 握手开销
// 3. LoadOrStore 保证并发安全，避免重复创建
var sharedReqClients sync.Map

// getSharedReqClient 获取共享的 req 客户端实例
// 性能优化：相同配置复用同一客户端，避免重复创建
func getSharedReqClient(opts reqClientOptions) (*req.Client, error) {
	key := buildReqClientKey(opts)
	if cached, ok := sharedReqClients.Load(key); ok {
		if c, ok := cached.(*req.Client); ok {
			return c, nil
		}
	}

	client := req.C().SetTimeout(opts.Timeout)
	if opts.ForceHTTP2 {
		client = client.EnableForceHTTP2()
	}
	if opts.Impersonate {
		client = client.ImpersonateChrome()
	}
	// req/v3 auto-decompresses gzip in its own transport; install a wrapper
	// that additionally handles deflate so callers can negotiate
	// `Accept-Encoding: gzip,deflate` (matches real Gemini CLI / Antigravity
	// fingerprint) and still receive readable bodies.
	client.WrapRoundTripFunc(deflateAwareRoundTrip)
	trimmed, _, err := proxyurl.Parse(opts.ProxyURL)
	if err != nil {
		return nil, err
	}
	if trimmed != "" {
		client.SetProxyURL(trimmed)
	}

	actual, _ := sharedReqClients.LoadOrStore(key, client)
	if c, ok := actual.(*req.Client); ok {
		return c, nil
	}
	return client, nil
}

func buildReqClientKey(opts reqClientOptions) string {
	return fmt.Sprintf("%s|%s|%t|%t",
		strings.TrimSpace(opts.ProxyURL),
		opts.Timeout.String(),
		opts.Impersonate,
		opts.ForceHTTP2,
	)
}

// CreatePrivacyReqClient creates an HTTP client for OpenAI privacy settings API
// This is exported for use by OpenAIPrivacyService
// Uses Chrome TLS fingerprint impersonation to bypass Cloudflare checks
func CreatePrivacyReqClient(proxyURL string) (*req.Client, error) {
	return getSharedReqClient(reqClientOptions{
		ProxyURL:    proxyURL,
		Timeout:     30 * time.Second,
		Impersonate: true, // Enable Chrome TLS fingerprint impersonation
	})
}

// deflateAwareRoundTrip wraps a req RoundTripFunc so that responses with
// `Content-Encoding: deflate` get streamed through a zlib/flate reader.
// Falls back to the raw body on decode failure so the caller still sees
// something useful in error logs.
func deflateAwareRoundTrip(rt req.RoundTripper) req.RoundTripFunc {
	return func(r *req.Request) (*req.Response, error) {
		resp, err := rt.RoundTrip(r)
		if err != nil || resp == nil || resp.Response == nil {
			return resp, err
		}
		if !strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "deflate") {
			return resp, nil
		}
		raw, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			return resp, nil
		}
		decoded, decErr := decompressDeflate(raw)
		if decErr != nil {
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			return resp, nil
		}
		resp.Body = io.NopCloser(bytes.NewReader(decoded))
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = int64(len(decoded))
		if resp.Response != nil {
			resp.Response.Header.Del("Content-Encoding")
			resp.Response.Header.Del("Content-Length")
			resp.Response.ContentLength = int64(len(decoded))
			resp.Response.Uncompressed = true
		}
		return resp, nil
	}
}

// decompressDeflate handles both RFC 1950 (zlib-wrapped) and RFC 1951
// (raw deflate) framings, since Google's CDN occasionally serves either.
func decompressDeflate(raw []byte) ([]byte, error) {
	if r, err := zlib.NewReader(bytes.NewReader(raw)); err == nil {
		defer func() { _ = r.Close() }()
		return io.ReadAll(r)
	}
	r := flate.NewReader(bytes.NewReader(raw))
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

// silence unused: net/http is referenced for type assertions when wrapping
// req's underlying transport in future helpers.
var _ http.RoundTripper = (*http.Transport)(nil)
