package service

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Request-shape validation for the antigravity_native paths.
//
// cloudcode-pa rejects any generateContent request whose final `contents` entry
// is a model turn: "Requests ending with a model turn are not supported."
// Probed live on 2026-09-04 — it holds for a trailing model turn that carries a
// signed function call, an unsigned function call, or plain text.
//
// The trap is the ERROR ORDERING. When that trailing model turn contains a
// function call with no `thoughtSignature`, the signature validator runs FIRST
// and the caller instead sees:
//
//	"Function call is missing a thought_signature in functionCall parts …
//	 Additional data, function call default_api:read, position 147"
//
// which sends people hunting for a signature bug. Verified in the same probe
// series that unsigned function calls are perfectly ACCEPTED (HTTP 200) in
// well-formed histories — mid-conversation and even as an unsigned secondary
// call in a signed-first parallel turn — so a missing signature is not itself
// the fault. Only the trailing model turn is.
//
// Detecting this locally converts a misleading upstream error into an accurate
// one AND keeps the request out of the failover loop, which would otherwise
// retry a permanently invalid payload across every account in the group.

// nativeTrailingModelTurnMessage is the actionable text handed to clients.
const nativeTrailingModelTurnMessage = "request ends with a model/assistant turn, which this upstream rejects: " +
	"append the tool result (functionResponse / tool_result) or a user turn before sending. " +
	"note that upstream may report this as a missing thought_signature, which is a symptom of the same trailing model turn"

// geminiRequestEndsWithModelTurn reports whether the final entry of a
// Gemini-shaped request body is a model turn, plus a short description of what
// that turn carries (for logs only — never message content).
//
// Accepts both the bare inner request (`contents` at the root) and the wrapped
// v1internal form (`request.contents`).
func geminiRequestEndsWithModelTurn(body []byte) (bool, string) {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return false, ""
	}
	contents := gjson.GetBytes(body, "contents")
	if !contents.IsArray() {
		contents = gjson.GetBytes(body, "request.contents")
	}
	if !contents.IsArray() {
		return false, ""
	}
	items := contents.Array()
	if len(items) == 0 {
		return false, ""
	}
	last := items[len(items)-1]
	role := strings.ToLower(strings.TrimSpace(last.Get("role").String()))
	if role != "model" && role != "assistant" {
		return false, ""
	}
	return true, describeGeminiTurnParts(last)
}

// describeGeminiTurnParts summarizes the part kinds in a turn. Only structural
// facts are reported: part kinds, counts and whether function calls carry a
// signature. No text, arguments or signatures are ever included.
func describeGeminiTurnParts(turn gjson.Result) string {
	var functionCalls, signedCalls, texts, thoughts, others int
	turn.Get("parts").ForEach(func(_, part gjson.Result) bool {
		switch {
		case part.Get("functionCall").Exists():
			functionCalls++
			if strings.TrimSpace(part.Get("thoughtSignature").String()) != "" {
				signedCalls++
			}
		case part.Get("thought").Bool():
			thoughts++
		case part.Get("text").Exists():
			texts++
		default:
			others++
		}
		return true
	})
	fields := make([]string, 0, 5)
	if functionCalls > 0 {
		fields = append(fields, "functionCalls="+itoa(functionCalls)+" signed="+itoa(signedCalls))
	}
	if thoughts > 0 {
		fields = append(fields, "thoughts="+itoa(thoughts))
	}
	if texts > 0 {
		fields = append(fields, "texts="+itoa(texts))
	}
	if others > 0 {
		fields = append(fields, "other="+itoa(others))
	}
	if len(fields) == 0 {
		return "empty"
	}
	return strings.Join(fields, " ")
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	digits := ""
	for v > 0 {
		digits = string(rune('0'+v%10)) + digits
		v /= 10
	}
	return digits
}

// writeGeminiProtocolError writes a Gemini-shaped error envelope and marks the
// response committed so the failover loop does not treat the request as
// retryable.
func writeGeminiProtocolError(c *gin.Context, status int, statusEnum, message string) error {
	MarkResponseCommitted(c)
	c.JSON(status, gin.H{
		"error": gin.H{
			"code":    status,
			"message": message,
			"status":  statusEnum,
		},
	})
	return nil
}
