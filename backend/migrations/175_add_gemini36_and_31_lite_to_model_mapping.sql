WITH additions(mapping) AS (
    VALUES ('{
        "gemini-3.1-flash-lite": "gemini-3.1-flash-lite",
        "gemini-3.6-flash": "gemini-3.6-flash",
        "gemini-3.6-flash-high": "gemini-3.6-flash-high",
        "gemini-3.6-flash-medium": "gemini-3.6-flash-medium",
        "gemini-3.6-flash-low": "gemini-3.6-flash-low"
    }'::jsonb)
)
UPDATE accounts AS a
SET credentials = jsonb_set(
        a.credentials,
        '{model_mapping}',
        additions.mapping || (a.credentials->'model_mapping'),
        false
    ),
    updated_at = NOW()
FROM additions
WHERE a.platform IN ('antigravity', 'antigravity_native')
  AND a.deleted_at IS NULL
  AND jsonb_typeof(a.credentials->'model_mapping') = 'object'
  AND a.credentials->'model_mapping' <> '{}'::jsonb
  AND EXISTS (
      SELECT 1
      FROM jsonb_object_keys(additions.mapping) AS model_key
      WHERE NOT (a.credentials->'model_mapping' ? model_key)
  );
