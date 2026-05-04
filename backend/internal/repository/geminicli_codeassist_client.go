package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/Wei-Shaw/sub2api/internal/pkg/googleapi"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/imroc/req/v3"
)

type geminiCliCodeAssistClient struct {
	baseURL string
}

func NewGeminiCliCodeAssistClient() service.GeminiCliCodeAssistClient {
	return &geminiCliCodeAssistClient{baseURL: geminicli.GeminiCliBaseURL}
}

func (c *geminiCliCodeAssistClient) LoadCodeAssist(ctx context.Context, accessToken, proxyURL string, reqBody *geminicli.LoadCodeAssistRequest) (*geminicli.LoadCodeAssistResponse, error) {
	if reqBody == nil {
		reqBody = defaultLoadCodeAssistRequest()
	}

	var out geminicli.LoadCodeAssistResponse
	client, err := createGeminiCliReqClient(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}
	apiURL := c.baseURL + "/v1internal:loadCodeAssist"
	fmt.Printf("[CodeAssist] LoadCodeAssist POST %s body=%+v\n", apiURL, reqBody)
	resp, err := client.R().
		SetContext(ctx).
		SetHeader("Authorization", "Bearer "+accessToken).
		SetHeader("Content-Type", "application/json").
		SetHeader("User-Agent", geminicli.BuildGeminiCLIUserAgent("")).
		SetHeader("x-goog-api-client", geminicli.GoogleAPIClientHeader).
		// Negotiate gzip,deflate to match the real Gemini CLI fingerprint.
		// req/v3 auto-decompresses gzip; deflate is handled by the
		// deflateDecompressMiddleware wired in via getSharedReqClient.
		SetHeader("Accept-Encoding", "gzip,deflate").
		SetBody(reqBody).
		SetSuccessResult(&out).
		Post(apiURL)
	if err != nil {
		fmt.Printf("[CodeAssist] LoadCodeAssist request error url=%s err=%v\n", apiURL, err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if !resp.IsSuccessState() {
		body := resp.String()
		sanitizedBody := geminicli.SanitizeBodyForLogs(body)
		contentEnc := resp.Header.Get("Content-Encoding")
		contentType := resp.Header.Get("Content-Type")
		fmt.Printf("[CodeAssist] LoadCodeAssist failed url=%s status=%d content-encoding=%q content-type=%q body=%s\n",
			apiURL, resp.StatusCode, contentEnc, contentType, sanitizedBody)

		// Check if this is a SERVICE_DISABLED error and extract activation URL
		if googleapi.IsServiceDisabledError(body) {
			activationURL := googleapi.ExtractActivationURL(body)
			if activationURL != "" {
				return nil, fmt.Errorf("gemini API not enabled for this project, please enable it by visiting: %s\n\nAfter enabling the API, wait a few minutes for the changes to propagate, then try again", activationURL)
			}
			return nil, fmt.Errorf("gemini API not enabled for this project, please enable it in the Google Cloud Console at: https://console.cloud.google.com/apis/library/cloudaicompanion.googleapis.com")
		}

		return nil, fmt.Errorf("loadCodeAssist failed: status %d, body: %s", resp.StatusCode, sanitizedBody)
	}
	fmt.Printf("[CodeAssist] LoadCodeAssist success: status %d, response: %+v\n", resp.StatusCode, out)
	return &out, nil
}

func (c *geminiCliCodeAssistClient) OnboardUser(ctx context.Context, accessToken, proxyURL string, reqBody *geminicli.OnboardUserRequest) (*geminicli.OnboardUserResponse, error) {
	if reqBody == nil {
		reqBody = defaultOnboardUserRequest()
	}

	var out geminicli.OnboardUserResponse
	client, err := createGeminiCliReqClient(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}
	apiURL := c.baseURL + "/v1internal:onboardUser"
	fmt.Printf("[CodeAssist] OnboardUser POST %s body=%+v\n", apiURL, reqBody)
	resp, err := client.R().
		SetContext(ctx).
		SetHeader("Authorization", "Bearer "+accessToken).
		SetHeader("Content-Type", "application/json").
		SetHeader("User-Agent", geminicli.BuildGeminiCLIUserAgent("")).
		SetHeader("x-goog-api-client", geminicli.GoogleAPIClientHeader).
		// gzip,deflate -- see LoadCodeAssist for the decompression chain.
		SetHeader("Accept-Encoding", "gzip,deflate").
		SetBody(reqBody).
		SetSuccessResult(&out).
		Post(apiURL)
	if err != nil {
		fmt.Printf("[CodeAssist] OnboardUser request error url=%s err=%v\n", apiURL, err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if !resp.IsSuccessState() {
		body := resp.String()
		sanitizedBody := geminicli.SanitizeBodyForLogs(body)
		contentEnc := resp.Header.Get("Content-Encoding")
		contentType := resp.Header.Get("Content-Type")
		fmt.Printf("[CodeAssist] OnboardUser failed url=%s status=%d content-encoding=%q content-type=%q body=%s\n",
			apiURL, resp.StatusCode, contentEnc, contentType, sanitizedBody)

		// Check if this is a SERVICE_DISABLED error and extract activation URL
		if googleapi.IsServiceDisabledError(body) {
			activationURL := googleapi.ExtractActivationURL(body)
			if activationURL != "" {
				return nil, fmt.Errorf("gemini API not enabled for this project, please enable it by visiting: %s\n\nAfter enabling the API, wait a few minutes for the changes to propagate, then try again", activationURL)
			}
			return nil, fmt.Errorf("gemini API not enabled for this project, please enable it in the Google Cloud Console at: https://console.cloud.google.com/apis/library/cloudaicompanion.googleapis.com")
		}

		return nil, fmt.Errorf("onboardUser failed: status %d, body: %s", resp.StatusCode, sanitizedBody)
	}
	fmt.Printf("[CodeAssist] OnboardUser success: status %d, response: %+v\n", resp.StatusCode, out)
	return &out, nil
}

func createGeminiCliReqClient(proxyURL string) (*req.Client, error) {
	return getSharedReqClient(reqClientOptions{
		ProxyURL: proxyURL,
		Timeout:  30 * time.Second,
	})
}

func defaultLoadCodeAssistRequest() *geminicli.LoadCodeAssistRequest {
	return &geminicli.LoadCodeAssistRequest{
		Metadata: geminicli.LoadCodeAssistMetadata{
			IDEType:    "IDE_UNSPECIFIED",
			Platform:   "PLATFORM_UNSPECIFIED",
			PluginType: "GEMINI",
		},
	}
}

func defaultOnboardUserRequest() *geminicli.OnboardUserRequest {
	return &geminicli.OnboardUserRequest{
		TierID: "LEGACY",
		Metadata: geminicli.LoadCodeAssistMetadata{
			IDEType:    "IDE_UNSPECIFIED",
			Platform:   "PLATFORM_UNSPECIFIED",
			PluginType: "GEMINI",
		},
	}
}
