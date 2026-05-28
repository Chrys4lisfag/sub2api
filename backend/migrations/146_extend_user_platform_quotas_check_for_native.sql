-- Extend the user_platform_quotas.platform CHECK constraint to accept the
-- new `antigravity_native` platform alongside the legacy `antigravity`.
-- Without this, INSERT/UPDATE of a per-user quota row for a native account
-- raises a constraint violation at the database layer even though the Go
-- domain layer accepts the value.
--
-- Migration 142 originally declared:
--   CHECK (platform IN ('anthropic', 'openai', 'gemini', 'antigravity'))
-- We drop and re-add it (Postgres has no in-place ALTER CHECK).

BEGIN;

ALTER TABLE user_platform_quotas
    DROP CONSTRAINT IF EXISTS user_platform_quotas_platform_check;

ALTER TABLE user_platform_quotas
    ADD CONSTRAINT user_platform_quotas_platform_check
    CHECK (platform IN ('anthropic', 'openai', 'gemini', 'antigravity', 'antigravity_native'));

COMMIT;
