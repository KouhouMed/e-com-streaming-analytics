-- ─────────────────────────────────────────────────────────────────────────────
-- Executed once automatically on first Postgres container startup.
-- Safe to re-run manually on an existing instance — every statement is
-- idempotent (IF NOT EXISTS).
--
-- Manual apply (if Postgres is already running):
--   docker exec -i postgres psql -U ecom_user -d ecom_db < db/init.sql
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS ecom;

-- ── Day 5: Streaming sink tables ──────────────────────────────────────────────

-- 1-minute tumbling window — total purchase revenue per window
-- Primary key is the window boundary pair; upserts are safe and lock-free
-- (only row-level locks via ON CONFLICT DO UPDATE).
CREATE TABLE IF NOT EXISTS ecom.hourly_revenue (
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    total_revenue   NUMERIC(12,2) NOT NULL DEFAULT 0,
    purchase_count  INTEGER       NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end)
);

-- 5-minute sliding window — view / purchase event counts per category
-- Each (window_start, window_end, category, action) tuple is unique.
CREATE TABLE IF NOT EXISTS ecom.category_metrics (
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    category        VARCHAR(100)  NOT NULL,
    action          VARCHAR(50)   NOT NULL,
    event_count     INTEGER       NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, category, action)
);

-- ── Indexes for Day 6-7 dashboard queries ────────────────────────────────────
-- Dashboard reads are always time-ordered DESC, so index on window_start DESC.
CREATE INDEX IF NOT EXISTS idx_hourly_revenue_window_start
    ON ecom.hourly_revenue (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_category_metrics_window_start
    ON ecom.category_metrics (window_start DESC, category, action);

-- ─────────────────────────────────────────────────────────────────────────────
DO $$
BEGIN
  RAISE NOTICE 'ecom schema initialised — hourly_revenue and category_metrics ready.';
END
$$;
