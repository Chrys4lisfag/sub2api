-- Add gemini-3.5-flash-{high,medium} thinking-level variants to existing
-- antigravity model_mapping rows. Mirrors router-for-me/CLIProxyAPI PR #3490
-- model registry additions.
--
-- Background:
-- The 3.5 Flash variants (high/medium) are public model IDs that map to
-- internal Antigravity wire-model names (gemini-3-flash-agent and
-- gemini-3.5-flash-low respectively). The wire-name translation happens in
-- pkg/antigravity/wire_model.go::AntigravityWireModel; this migration just
-- registers the public IDs so per-account model_mapping rows accept them.
--
-- Strategy:
-- Mirror migrations 058..144 — overwrite the full model_mapping so DB stays
-- in lockstep with DefaultAntigravityModelMapping in constants.go.

UPDATE accounts
SET credentials = jsonb_set(
    credentials,
    '{model_mapping}',
    '{
        "claude-opus-4-7": "claude-opus-4-7",
        "claude-opus-4-6-thinking": "claude-opus-4-6-thinking",
        "claude-opus-4-6": "claude-opus-4-6-thinking",
        "claude-opus-4-5-thinking": "claude-opus-4-6-thinking",
        "claude-opus-4-5-20251101": "claude-opus-4-6-thinking",
        "claude-sonnet-4-6": "claude-sonnet-4-6",
        "claude-sonnet-4-5": "claude-sonnet-4-5",
        "claude-sonnet-4-5-thinking": "claude-sonnet-4-5-thinking",
        "claude-sonnet-4-5-20250929": "claude-sonnet-4-5",
        "claude-haiku-4-5": "claude-sonnet-4-5",
        "claude-haiku-4-5-20251001": "claude-sonnet-4-5",
        "gemini-2.5-flash": "gemini-2.5-flash",
        "gemini-2.5-flash-image": "gemini-2.5-flash-image",
        "gemini-2.5-flash-image-preview": "gemini-2.5-flash-image",
        "gemini-2.5-flash-lite": "gemini-2.5-flash-lite",
        "gemini-2.5-flash-thinking": "gemini-2.5-flash-thinking",
        "gemini-2.5-pro": "gemini-2.5-pro",
        "gemini-3-flash": "gemini-3-flash",
        "gemini-3-pro-high": "gemini-3-pro-high",
        "gemini-3-pro-low": "gemini-3-pro-low",
        "gemini-3-flash-preview": "gemini-3.5-flash",
        "gemini-3-pro-preview": "gemini-3-pro-high",
        "gemini-3.1-pro-high": "gemini-3.1-pro-high",
        "gemini-3.1-pro-low": "gemini-3.1-pro-low",
        "gemini-3.1-pro-preview": "gemini-3.1-pro-high",
        "gemini-3.1-flash-image": "gemini-3.1-flash-image",
        "gemini-3.1-flash-image-preview": "gemini-3.1-flash-image",
        "gemini-3-pro-image": "gemini-3.1-flash-image",
        "gemini-3-pro-image-preview": "gemini-3.1-flash-image",
        "gemini-3.5-flash": "gemini-3.5-flash",
        "gemini-3.5-flash-high": "gemini-3.5-flash-high",
        "gemini-3.5-flash-medium": "gemini-3.5-flash-medium",
        "gpt-oss-120b-medium": "gpt-oss-120b-medium",
        "tab_flash_lite_preview": "tab_flash_lite_preview"
    }'::jsonb
)
WHERE platform = 'antigravity'
  AND deleted_at IS NULL
  AND credentials->'model_mapping' IS NOT NULL;
