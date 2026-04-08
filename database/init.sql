

-- Prevent duplicate entries (important for upsert)
CREATE UNIQUE INDEX IF NOT EXISTS unique_stock_entry
ON stock_prices(symbol, timestamp);

------------------------------------------------------------

-- -- Table to log ingestion activity
-- CREATE TABLE IF NOT EXISTS ingestion_logs (
--     id SERIAL PRIMARY KEY,
--     symbol VARCHAR(10),
--     message TEXT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );\\

CREATE TABLE ingestion_logs (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    event_type VARCHAR(50),
    status VARCHAR(20),
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    date_time TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);