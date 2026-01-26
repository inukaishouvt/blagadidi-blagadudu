-- Phase 4: Data Modeling - Star Schema

-- 1. Dimension: Platform
-- Represents the source platform (Meta, TikTok, etc.)
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(50) UNIQUE NOT NULL
);

-- 2. Dimension: Time
-- Granualarity: Hourly (based on source data)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    timestamp_utc TIMESTAMP UNIQUE NOT NULL,
    year INT,
    month INT,
    day INT,
    hour INT,
    day_of_week INT
);

-- 3. Dimension: Customer (Account)
-- Derived from distinct account IDs found in source
CREATE TABLE IF NOT EXISTS dim_account (
    account_id SERIAL PRIMARY KEY,
    source_account_id VARCHAR(255) UNIQUE NOT NULL, -- The ID from the CSV
    account_name VARCHAR(255) -- Optional if we had it
);

-- 4. Dimension: Ad Type (Creative/Format)
-- Derived from ad_lifecycle_status or other metadata if available. 
-- Note: 'ad_lifecycle_status' changes over time, so it might be on the Fact or a Type 2 Dim. 
-- However, 'Ad Format' (Video, Image) isn't explicitly unified yet, but we can placeholder 'Ad Status' as a dimension.
CREATE TABLE IF NOT EXISTS dim_ad_status (
    status_id SERIAL PRIMARY KEY,
    lifecycle_status VARCHAR(50) UNIQUE
);

-- 5. Dimension: Device
CREATE TABLE IF NOT EXISTS dim_device (
    device_id SERIAL PRIMARY KEY,
    device_type VARCHAR(50) UNIQUE NOT NULL
);

-- 6. Fact Table: Hourly Ad Performance
CREATE TABLE IF NOT EXISTS fact_ad_performance (
    fact_id BIGSERIAL PRIMARY KEY,
    ad_id VARCHAR(255) NOT NULL, -- Business Key
    
    -- Foreign Keys
    platform_id INT REFERENCES dim_platform(platform_id),
    time_id INT REFERENCES dim_time(time_id),
    account_id INT REFERENCES dim_account(account_id),
    status_id INT REFERENCES dim_ad_status(status_id),
    device_id INT REFERENCES dim_device(device_id),
    
    -- Metrics
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    spend NUMERIC(18, 6) DEFAULT 0, -- High precision for currency
    currency_code VARCHAR(10) DEFAULT 'USD',
    video_views INT DEFAULT 0,
    
    -- Metadata
    pipeline_status VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_ad_performance(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_platform ON fact_ad_performance(platform_id);
