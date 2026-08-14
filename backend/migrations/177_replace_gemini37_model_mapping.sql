WITH defaults(mapping) AS (
    VALUES ('{
        "gemini-3.7-flash-high": "gemini-3.7-flash-high",
        "gemini-3.7-flash-medium": "gemini-3.7-flash-medium",
        "gemini-3.7-flash-low": "gemini-3.7-flash-low"
    }'::jsonb)
)
UPDATE accounts AS a
SET credentials = jsonb_set(
        a.credentials,
        '{model_mapping}',
        CASE
            WHEN a.credentials->'model_mapping'->>'gemini-3.7-flash' = 'gemini-3.7-flash'
            THEN (defaults.mapping || (a.credentials->'model_mapping')) - 'gemini-3.7-flash'
            ELSE (defaults.mapping || (a.credentials->'model_mapping'))
        END,
        false
    ),
    updated_at = NOW()
FROM defaults
WHERE a.platform IN ('antigravity', 'antigravity_native')
  AND a.deleted_at IS NULL
  AND jsonb_typeof(a.credentials->'model_mapping') = 'object'
  AND a.credentials->'model_mapping' <> '{}'::jsonb;
