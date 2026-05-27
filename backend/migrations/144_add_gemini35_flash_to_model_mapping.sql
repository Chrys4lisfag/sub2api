-- Add gemini-3.5-flash (GA 2026-05-12) to existing antigravity model_mapping rows.
--
-- Background:
-- Gemini 3.5 Flash was released as stable on 2026-05-12 per
-- https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash. The model
-- code is `gemini-3.5-flash`. Per Google's "What's new in Gemini 3.5 Flash"
-- guide, this model carries forward the `gemini-3-flash-preview` alias so we
-- map both to the GA codename for forward-compat with clients pinning the
-- preview name.
--
-- Strategy:
-- Mirror the previous migrations (058/059/060/071): overwrite the entire
-- model_mapping with the new set so DB stays in lockstep with
-- DefaultAntigravityModelMapping in constants.go.

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
        "gpt-oss-120b-medium": "gpt-oss-120b-medium",
        "tab_flash_lite_preview": "tab_flash_lite_preview"
    }'::jsonb
)
WHERE platform = 'antigravity'
  AND deleted_at IS NULL
  AND credentials->'model_mapping' IS NOT NULL;
