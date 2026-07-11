package repository

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
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
	// req/v3 disables its own gzip auto-decompression when the caller sets
	// `Accept-Encoding` manually. We send `gzip,deflate` to match real Gemini
	// CLI / Antigravity fingerprint, so install a wrapper that decodes both
	// encodings ourselves; otherwise response bodies arrive raw and JSON
	// unmarshal blows up at the first `\x1f` magic byte.
	client.WrapRoundTripFunc(decompressionAwareRoundTrip)
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

// decompressionAwareRoundTrip wraps a req RoundTripper so that responses
// with `Content-Encoding: gzip` or `deflate` get streamed through the
// matching decoder. req/v3's own auto-gzip is bypassed once the caller
// pins Accept-Encoding manually -- which we do to match the real Gemini
// CLI / Antigravity fingerprint -- so we have to handle both encodings
// here. Falls back to the raw body on decode failure so error logs still
// see something.
func decompressionAwareRoundTrip(rt req.RoundTripper) req.RoundTripFunc {
	return func(r *req.Request) (*req.Response, error) {
		resp, err := rt.RoundTrip(r)
		if err != nil || resp == nil || resp.Response == nil {
			return resp, err
		}
		enc := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
		raw, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			return resp, nil
		}
		// Some Google cloudcode-pa endpoints (loadCodeAssist / onboardUser)
		// return gzip-compressed bodies WITHOUT a Content-Encoding header.
		// Sniff magic bytes when the header is missing/unrecognized so we
		// still decode instead of leaking raw gzip into JSON parsers.
		if enc != "gzip" && enc != "deflate" {
			sniffed := sniffCompressedEncoding(raw)
			if sniffed == "" {
				resp.Body = io.NopCloser(bytes.NewReader(raw))
				return resp, nil
			}
			enc = sniffed
		}
		decoded, decErr := decompressBody(enc, raw)
		if decErr != nil {
			resp.Body = io.NopCloser(bytes.NewReader(raw))
			return resp, nil
		}
		resp.Body = io.NopCloser(bytes.NewReader(decoded))
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = int64(len(decoded))
		if resp.Response != nil {
			resp.Uncompressed = true
		}
		return resp, nil
	}
}

// decompressBody handles `gzip` (RFC 1952), `deflate` zlib-wrapped (RFC
// 1950), and `deflate` raw (RFC 1951) framings. Some Google services flip
// between deflate framings, hence the dual-fallback for that branch.
func decompressBody(encoding string, raw []byte) ([]byte, error) {
	switch encoding {
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		defer func() { _ = gr.Close() }()
		return io.ReadAll(gr)
	case "deflate":
		if zr, err := zlib.NewReader(bytes.NewReader(raw)); err == nil {
			defer func() { _ = zr.Close() }()
			return io.ReadAll(zr)
		}
		fr := flate.NewReader(bytes.NewReader(raw))
		defer func() { _ = fr.Close() }()
		return io.ReadAll(fr)
	default:
		return raw, nil
	}
}

// sniffCompressedEncoding detects gzip (RFC 1952) and zlib-wrapped deflate
// (RFC 1950) by leading magic bytes. Returns "" when the body does not
// look compressed. Raw RFC 1951 deflate has no magic header and is not
// sniffed here -- callers that expect raw deflate should set
// Content-Encoding explicitly.
func sniffCompressedEncoding(raw []byte) string {
	if len(raw) < 2 {
		return ""
	}
	// gzip magic: 1f 8b
	if raw[0] == 0x1f && raw[1] == 0x8b {
		return "gzip"
	}
	// zlib header: first byte low nibble = 8 (deflate); valid second
	// bytes are constrained so that (raw[0]<<8 | raw[1]) % 31 == 0.
	// Practically: 0x78 0x01 / 0x5e / 0x9c / 0xda are the dominant combos.
	if raw[0]&0x0f == 0x08 && (uint16(raw[0])<<8|uint16(raw[1]))%31 == 0 {
		return "deflate"
	}
	return ""
}

// silence unused: net/http is referenced for type assertions when wrapping
// req's underlying transport in future helpers.
var _ http.RoundTripper = (*http.Transport)(nil)
