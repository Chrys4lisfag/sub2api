-- Add base `gemini-3.1-pro` to model_mapping so single-picker entries
-- with thinking-level slider can resolve through account selection.
--
-- Background:
-- Until 147 the mapping only carried -high / -low / -preview suffixed
-- variants. omp now ships a single `gemini-3.1-pro` picker entry whose
-- thinking slider drives wire-model selection via wire_model.go::
-- ResolveWireFromBody. Without the base name in model_mapping the
-- account-selection filter (mapAntigravityModel) rejects every account.
--
-- Strategy:
-- Overwrite the full model_mapping so DB stays in lockstep with
-- DefaultAntigravityModelMapping in constants.go.
--
-- Also extends WHERE to platform IN ('antigravity', 'antigravity_native')
-- so the native backend's accounts get the same default mapping. Prior
-- migrations (058..147) only touched legacy `antigravity` accounts; for
-- the native fleet the in-code default
-- resolveModelMapping(rawMapping) → DefaultAntigravityModelMapping
-- still applies because resolveModelMapping is now ag-family aware.

UPDATE accounts
SET credentials = jsonb_set(
    credentials,
    '{model_mapping}',
    '{
        "claude-opus-4-8": "claude-opus-4-8",
        "claude-opus-4-7": "claude-opus-4-7",
        "claude-opus-4-6-thinking": "claude-opus-4-6-thinking",
        "claude-opus-4-6": "claude-opus-4-6-thinking",
        "claude-opus-4-5-thinking": "claude-opus-4-6-thinking",
        "claude-opus-4-5-20251101": "claude-opus-4-6-thinking",
        "claude-sonnet-4-6": "claude-sonnet-4-6",
        "claude-sonnet-4-5": "claude-sonnet-4-5",
        "claude-sonnet-4-5-thinking": "claude-sonnet-4-5-thinking",
        "claude-sonnet-4-5-20250929": "claude-sonnet-4-5",
        "claude-haiku-4-5": "claude-sonnet-4-6",
        "claude-haiku-4-5-20251001": "claude-sonnet-4-6",
        "gemini-2.5-flash": "gemini-2.5-flash",
        "gemini-2.5-flash-image": "gemini-2.5-flash-image",
        "gemini-2.5-flash-image-preview": "gemini-2.5-flash-image",
        "gemini-2.5-flash-lite": "gemini-2.5-flash-lite",
        "gemini-2.5-flash-thinking": "gemini-2.5-flash-thinking",
        "gemini-2.5-pro": "gemini-2.5-pro",
        "gemini-3-flash": "gemini-3-flash",
        "gemini-3-flash-preview": "gemini-3-flash",
        "gemini-3.1-pro": "gemini-3.1-pro",
        "gemini-3.1-pro-high": "gemini-3.1-pro-high",
        "gemini-3.1-pro-low": "gemini-3.1-pro-low",
        "gemini-3.1-pro-preview": "gemini-3.1-pro-high",
        "gemini-3.1-flash-image": "gemini-3.1-flash-image",
        "gemini-3.1-flash-image-preview": "gemini-3.1-flash-image",
        "gemini-3.5-flash": "gemini-3.5-flash",
        "gemini-3.5-flash-high": "gemini-3.5-flash-high",
        "gemini-3.5-flash-medium": "gemini-3.5-flash-medium",
        "gemini-3.5-flash-low": "gemini-3.5-flash-low",
        "gpt-oss-120b-medium": "gpt-oss-120b-medium",
        "tab_flash_lite_preview": "tab_flash_lite_preview"
    }'::jsonb
)
WHERE platform IN ('antigravity', 'antigravity_native')
  AND deleted_at IS NULL
  AND credentials->'model_mapping' IS NOT NULL;
