// Package types describes the wire format Antigravity expects on
// daily-cloudcode-pa.sandbox.googleapis.com/v1internal:{generate,stream,count}.
//
// The shape is Gemini-style — request.contents[].role+parts[], generationConfig
// with thinkingConfig, function tools as functionDeclarations.
package types

// Request is the top-level envelope for :generateContent / :streamGenerateContent.
type Request struct {
	Project   string         `json:"project,omitempty"`     // cloudaicompanion project ID
	Model     string         `json:"model"`                 // e.g. "gemini-3-pro-high"
	Request   GenerateInner  `json:"request"`               // inner request body
	UserAgent string         `json:"userAgent,omitempty"`   // literal "antigravity"
	RequestID string         `json:"requestId,omitempty"`   // "agent-<uuid>"
}

// GenerateInner mirrors the Vertex AI GenerateContent inner payload.
type GenerateInner struct {
	Contents          []Content          `json:"contents"`
	SystemInstruction *SystemInstruction `json:"systemInstruction,omitempty"`
	Tools             []Tool             `json:"tools,omitempty"`
	ToolConfig        *ToolConfig        `json:"toolConfig,omitempty"`
	GenerationConfig  *GenerationConfig  `json:"generationConfig,omitempty"`
	SessionID         string             `json:"sessionId,omitempty"`
}

// Content is one turn in the conversation.
type Content struct {
	Role  string `json:"role"`  // "user" | "model"
	Parts []Part `json:"parts"`
}

// Part is a polymorphic message fragment. Only one field is set.
type Part struct {
	Text             string            `json:"text,omitempty"`
	InlineData       *InlineData       `json:"inlineData,omitempty"`
	FunctionCall     *FunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *FunctionResponse `json:"functionResponse,omitempty"`
	Thought          bool              `json:"thought,omitempty"`
	ThoughtSignature string            `json:"thoughtSignature,omitempty"`
}

type InlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"` // base64
}

type FunctionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
	ID   string         `json:"id,omitempty"` // tool-use ID echoed back in functionResponse
}

type FunctionResponse struct {
	Name     string         `json:"name"`
	ID       string         `json:"id,omitempty"`
	Response map[string]any `json:"response"`
}

// SystemInstruction must be the {parts: [...]} object form, not a string.
type SystemInstruction struct {
	Parts []Part `json:"parts"`
}

type Tool struct {
	FunctionDeclarations []FunctionDeclaration `json:"functionDeclarations,omitempty"`
	GoogleSearch         *struct{}             `json:"googleSearch,omitempty"`
	URLContext           *struct{}             `json:"urlContext,omitempty"`
}

type FunctionDeclaration struct {
	Name        string         `json:"name"`        // 1st char [a-zA-Z_], rest [a-zA-Z0-9_.:-], <=64 chars
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"` // JSON-schema object; NO $ref/$defs/const/default/examples
}

type ToolConfig struct {
	FunctionCallingConfig *FunctionCallingConfig `json:"functionCallingConfig,omitempty"`
}

type FunctionCallingConfig struct {
	Mode                 string   `json:"mode,omitempty"` // AUTO | ANY | NONE | VALIDATED
	AllowedFunctionNames []string `json:"allowedFunctionNames,omitempty"`
}

type GenerationConfig struct {
	Temperature      *float64        `json:"temperature,omitempty"`
	TopP             *float64        `json:"topP,omitempty"`
	TopK             *int            `json:"topK,omitempty"`
	MaxOutputTokens  int             `json:"maxOutputTokens,omitempty"`
	StopSequences    []string        `json:"stopSequences,omitempty"`
	ThinkingConfig   *ThinkingConfig `json:"thinkingConfig,omitempty"`
}

type ThinkingConfig struct {
	ThinkingBudget  int  `json:"thinkingBudget,omitempty"`
	IncludeThoughts bool `json:"includeThoughts,omitempty"`
}

// Response is the wrapper returned by both generate and stream endpoints.
type Response struct {
	Response StreamResponse `json:"response"`
	TraceID  string         `json:"traceId,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// StreamResponse holds the candidates+usage.
type StreamResponse struct {
	Candidates    []Candidate    `json:"candidates,omitempty"`
	UsageMetadata *UsageMetadata `json:"usageMetadata,omitempty"`
	ModelVersion  string         `json:"modelVersion,omitempty"`
	ResponseID    string         `json:"responseId,omitempty"`
}

type Candidate struct {
	Content           Content            `json:"content"`
	FinishReason      string             `json:"finishReason,omitempty"` // STOP | MAX_TOKENS | OTHER
	GroundingMetadata *GroundingMetadata `json:"groundingMetadata,omitempty"`
}

type UsageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount,omitempty"`
	CandidatesTokenCount int `json:"candidatesTokenCount,omitempty"`
	TotalTokenCount      int `json:"totalTokenCount,omitempty"`
	ThoughtsTokenCount   int `json:"thoughtsTokenCount,omitempty"`
}

type GroundingMetadata struct {
	WebSearchQueries  []string          `json:"webSearchQueries,omitempty"`
	SearchEntryPoint  map[string]any    `json:"searchEntryPoint,omitempty"`
	GroundingChunks   []GroundingChunk  `json:"groundingChunks,omitempty"`
	GroundingSupports []GroundingSupport `json:"groundingSupports,omitempty"`
}

type GroundingChunk struct {
	Web *struct {
		URI   string `json:"uri,omitempty"`
		Title string `json:"title,omitempty"`
	} `json:"web,omitempty"`
}

type GroundingSupport struct {
	Segment              map[string]any `json:"segment,omitempty"`
	GroundingChunkIndices []int         `json:"groundingChunkIndices,omitempty"`
}

// APIError is the standard Google error envelope.
type APIError struct {
	Error struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Status  string         `json:"status"`
		Details []any          `json:"details,omitempty"`
	} `json:"error"`
}
