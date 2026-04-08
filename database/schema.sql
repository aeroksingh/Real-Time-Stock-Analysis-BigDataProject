-- =============================================================
-- schema.sql  –  Run manually or via init_db.py
-- =============================================================

CREATE TABLE IF NOT EXISTS raw_stock_data (
    id          BIGSERIAL PRIMARY KEY,
    ticker      VARCHAR(10)     NOT NULL,
    date_time   TIMESTAMPTZ     NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    source      VARCHAR(50)     DEFAULT 'yfinance',
    created_at  TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (ticker, date_time)
);

CREATE INDEX IF NOT EXISTS idx_raw_ticker       ON raw_stock_data (ticker);
CREATE INDEX IF NOT EXISTS idx_raw_date_time    ON raw_stock_data (date_time DESC);

-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS processed_stock_data (
    id           BIGSERIAL PRIMARY KEY,
    ticker       VARCHAR(10)     NOT NULL,
    date_time    TIMESTAMPTZ     NOT NULL,
    open         NUMERIC(12,4),
    high         NUMERIC(12,4),
    low          NUMERIC(12,4),
    close        NUMERIC(12,4),
    volume       BIGINT,
    daily_return NUMERIC(10,6),
    ma_7         NUMERIC(12,4),
    ma_30        NUMERIC(12,4),
    volatility   NUMERIC(10,6),
    trend_label  VARCHAR(10),
    created_at   TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (ticker, date_time)
);

CREATE INDEX IF NOT EXISTS idx_proc_ticker      ON processed_stock_data (ticker);
CREATE INDEX IF NOT EXISTS idx_proc_date_time   ON processed_stock_data (date_time DESC);
CREATE INDEX IF NOT EXISTS idx_proc_trend       ON processed_stock_data (trend_label);

-- ---------------------------------------------------------------

-- ---------------------------------------------------------------
-- Lightweight table for streaming consumer / dashboard
-- stores the latest raw price points for simple dashboards
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stock_prices (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10)     NOT NULL,
    price       NUMERIC(12,4),
    timestamp   TIMESTAMPTZ     NOT NULL,
    created_at  TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol   ON stock_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp ON stock_prices (timestamp DESC);

-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion_logs (
    id          BIGSERIAL PRIMARY KEY,
    ticker      VARCHAR(10),
    event_type  VARCHAR(50),
    status      VARCHAR(20),
    message     TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logs_ticker      ON ingestion_logs (ticker);
CREATE INDEX IF NOT EXISTS idx_logs_created_at  ON ingestion_logs (created_at DESC);