WITH defaults(mapping) AS (
    VALUES ('{
        "gemini-3.7-flash": "gemini-3.7-flash"
    }'::jsonb)
)
UPDATE accounts AS a
SET credentials = jsonb_set(
        a.credentials,
        '{model_mapping}',
        defaults.mapping || (a.credentials->'model_mapping'),
        false
    ),
    updated_at = NOW()
FROM defaults
WHERE a.platform IN ('antigravity', 'antigravity_native')
  AND a.deleted_at IS NULL
  AND jsonb_typeof(a.credentials->'model_mapping') = 'object'
  AND a.credentials->'model_mapping' <> '{}'::jsonb;
