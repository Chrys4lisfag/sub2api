WITH additions(mapping) AS (
    VALUES ('{
        "gemini-3.7-flash": "gemini-3.7-flash"
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
