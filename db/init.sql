-- ─────────────────────────────────────────────────────────────────────────────
-- Base schema — executed once on first Postgres startup.
-- Table definitions are added incrementally from Day 3 onwards.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS ecom;

-- Confirm init ran successfully (visible in postgres logs)
DO $$
BEGIN
  RAISE NOTICE 'ecom schema initialised successfully.';
END
$$;
