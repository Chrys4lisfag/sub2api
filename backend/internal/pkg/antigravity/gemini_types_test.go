package antigravity

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestV1InternalRequestLabelsRoundTrip(t *testing.T) {
	fixture := []byte(`{
		"project":"redacted-project",
		"requestId":"agent/redacted",
		"userAgent":"antigravity",
		"requestType":"agent",
		"model":"gemini-3-flash-agent",
		"request":{
			"contents":[],
			"labels":{
				"request_origin":"agent",
				"trajectory_id":"redacted"
			}
		}
	}`)

	var request V1InternalRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	wantLabels := map[string]string{
		"request_origin": "agent",
		"trajectory_id":  "redacted",
	}
	if !reflect.DeepEqual(request.Request.Labels, wantLabels) {
		t.Fatalf("labels after unmarshal: got %#v, want %#v", request.Request.Labels, wantLabels)
	}

	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatalf("unmarshal fixture for comparison: %v", err)
	}
	if err := json.Unmarshal(outbound, &got); err != nil {
		t.Fatalf("unmarshal outbound request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("typed round trip changed envelope:\n got: %s\nwant: %s", outbound, fixture)
	}
}

func TestV1InternalRequestLabelsOmittedWhenAbsent(t *testing.T) {
	fixture := []byte(`{
		"project":"redacted-project",
		"requestId":"checkpoint/redacted",
		"userAgent":"antigravity",
		"requestType":"checkpoint",
		"model":"gemini-3-flash",
		"request":{"contents":[]}
	}`)

	var request V1InternalRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(outbound, &envelope); err != nil {
		t.Fatalf("unmarshal outbound envelope: %v", err)
	}
	var inner map[string]json.RawMessage
	if err := json.Unmarshal(envelope["request"], &inner); err != nil {
		t.Fatalf("unmarshal outbound inner request: %v", err)
	}
	if _, present := inner["labels"]; present {
		t.Fatalf("outbound request unexpectedly contains labels: %s", outbound)
	}
}

func TestV1InternalRequestOptionalConfigFieldsRoundTrip(t *testing.T) {
	fixture := []byte(`{
		"project":"redacted-project",
		"requestId":"agent/redacted",
		"userAgent":"antigravity",
		"requestType":"agent",
		"model":"gemini-3-flash-agent",
		"request":{
			"contents":[],
			"generationConfig":{
				"candidateCount":2,
				"responseModalities":["TEXT","IMAGE"],
				"thinkingConfig":{
					"includeThoughts":true,
					"thinkingBudget":0,
					"thinkingLevel":"low"
				}
			},
			"toolConfig":{
				"functionCallingConfig":{
					"mode":"VALIDATED",
					"allowedFunctionNames":["first_tool","second_tool"]
				}
			}
		}
	}`)

	var request V1InternalRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	config := request.Request.GenerationConfig
	if config == nil {
		t.Fatal("generationConfig is nil")
	}
	if config.CandidateCount == nil || *config.CandidateCount != 2 {
		t.Fatalf("candidateCount: got %v, want 2", config.CandidateCount)
	}
	if config.ResponseModalities == nil || !reflect.DeepEqual(*config.ResponseModalities, []string{"TEXT", "IMAGE"}) {
		t.Fatalf("responseModalities: got %#v", config.ResponseModalities)
	}
	if config.ThinkingConfig == nil {
		t.Fatal("thinkingConfig is nil")
	}
	if config.ThinkingConfig.ThinkingBudget == nil || *config.ThinkingConfig.ThinkingBudget != 0 {
		t.Fatalf("thinkingBudget: got %v, want explicit 0", config.ThinkingConfig.ThinkingBudget)
	}
	if config.ThinkingConfig.ThinkingLevel == nil || *config.ThinkingConfig.ThinkingLevel != "low" {
		t.Fatalf("thinkingLevel: got %v, want low", config.ThinkingConfig.ThinkingLevel)
	}
	functionConfig := request.Request.ToolConfig.FunctionCallingConfig
	if functionConfig.AllowedFunctionNames == nil || !reflect.DeepEqual(*functionConfig.AllowedFunctionNames, []string{"first_tool", "second_tool"}) {
		t.Fatalf("allowedFunctionNames: got %#v", functionConfig.AllowedFunctionNames)
	}

	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatalf("unmarshal fixture for comparison: %v", err)
	}
	if err := json.Unmarshal(outbound, &got); err != nil {
		t.Fatalf("unmarshal outbound request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("typed round trip changed optional config fields:\n got: %s\nwant: %s", outbound, fixture)
	}
}

func TestGeminiThinkingBudgetOmittedWhenAbsent(t *testing.T) {
	fixture := []byte(`{
		"contents":[],
		"generationConfig":{
			"thinkingConfig":{
				"includeThoughts":false,
				"thinkingLevel":"medium"
			}
		}
	}`)

	var request GeminiRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if request.GenerationConfig.ThinkingConfig.ThinkingBudget != nil {
		t.Fatalf("absent thinkingBudget decoded as %v", request.GenerationConfig.ThinkingConfig.ThinkingBudget)
	}
	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatalf("unmarshal fixture for comparison: %v", err)
	}
	if err := json.Unmarshal(outbound, &got); err != nil {
		t.Fatalf("unmarshal outbound request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("absent thinkingBudget was not preserved:\n got: %s\nwant: %s", outbound, fixture)
	}
}

func TestGeminiOptionalEmptyArraysRemainPresent(t *testing.T) {
	fixture := []byte(`{
		"contents":[],
		"generationConfig":{
			"responseModalities":[],
			"stopSequences":[]
		},
		"toolConfig":{
			"functionCallingConfig":{
				"allowedFunctionNames":[]
			}
		}
	}`)

	var request GeminiRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatalf("unmarshal fixture for comparison: %v", err)
	}
	if err := json.Unmarshal(outbound, &got); err != nil {
		t.Fatalf("unmarshal outbound request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit empty arrays were not preserved:\n got: %s\nwant: %s", outbound, fixture)
	}
}

func TestGeminiIncludeThoughtsOmittedWhenAbsent(t *testing.T) {
	fixture := []byte(`{
		"contents":[],
		"generationConfig":{
			"thinkingConfig":{
				"thinkingBudget":0
			}
		}
	}`)

	var request GeminiRequest
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	outbound, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatalf("unmarshal fixture for comparison: %v", err)
	}
	if err := json.Unmarshal(outbound, &got); err != nil {
		t.Fatalf("unmarshal outbound request: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("absent includeThoughts was not preserved:\n got: %s\nwant: %s", outbound, fixture)
	}
}
