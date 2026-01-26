-- transformations.sql
-- Goal: Standardize and Clean data from raw_* tables into unified_ads

-- 0. Create Intermediary Tables (Drop first to ensure schema update)
DROP TABLE IF EXISTS unified_ads CASCADE;
CREATE TABLE unified_ads (
    ad_id VARCHAR(255),
    platform VARCHAR(50),
    timestamp_utc TIMESTAMP,
    impressions INT,
    clicks INT,
    spend NUMERIC(18, 6),
    currency VARCHAR(10),
    pipeline_status VARCHAR(50),
    ad_lifecycle_status VARCHAR(50),
    video_views INT,
    device_type VARCHAR(50)
);

DROP TABLE IF EXISTS ads_quarantine CASCADE;
CREATE TABLE ads_quarantine (
    ad_id VARCHAR(255),
    platform VARCHAR(50),
    timestamp_utc TIMESTAMP,
    pipeline_status VARCHAR(50),
    raw_record JSONB
);

-- Tables cleared by Drop/Create above

-- Helper to normalize status
-- (Doing inline via CASE for performance/simplicity or could be a function)

-- 1. META ADS
INSERT INTO unified_ads (platform, ad_id, timestamp_utc, impressions, clicks, spend, currency, pipeline_status, ad_lifecycle_status, video_views, device_type)
SELECT 
    'meta',
    meta_ad_id,
    -- Timestamp conversion (handling timezone - simplifying for SQL example, assuming UTC or specific offset logic needed)
    -- For now casting directly as the raw data might need more complex TZ logic if not already UTC. 
    -- Assuming Ingestion might have helped or we do basic cast. 
    -- Ideally we fix TZs. Let's try basic cast first.
    CAST(hour_start_local AS TIMESTAMP), 
    CAST(impressions AS INT),
    CAST(clicks_all AS INT),
    CAST(spend AS NUMERIC),
    currency,
    CASE 
        WHEN UPPER(pipeline_status) IN ('VAL', 'VALID') THEN 'VALIDATED'
        WHEN UPPER(pipeline_status) IN ('PUB') THEN 'PUBLISHED'
        WHEN UPPER(pipeline_status) IN ('QRN', 'QUAR') THEN 'QUARANTINED'
        ELSE UPPER(pipeline_status)
    END,
    delivery_status,
    CAST(video_view_2s AS INT),
    -- Device Type Standardization
    CASE 
        WHEN LOWER(device_platform) IN ('mobile', 'moblie') THEN 'mobile'
        WHEN LOWER(device_platform) IN ('desktop', 'deskotp') THEN 'desktop'
        ELSE 'unknown'
    END
FROM raw_meta_ads_hourly
WHERE UPPER(pipeline_status) NOT IN ('QRN', 'QUAR', 'ERROR', 'QUARANTINED')
  AND meta_ad_id IS NOT NULL;


-- 2. TIKTOK ADS
INSERT INTO unified_ads (platform, ad_id, timestamp_utc, impressions, clicks, spend, currency, pipeline_status, ad_lifecycle_status, video_views, device_type)
SELECT 
    'tiktok',
    ad_id,
    CAST(stat_time_hour AS TIMESTAMP), -- improved TZ logic needed if not UTC
    CAST(impressions AS INT),
    CAST(clicks AS INT),
    CAST(cost AS NUMERIC),
    currency_code,
    CASE 
        WHEN UPPER(pipe_state) IN ('VAL', 'VALID') THEN 'VALIDATED'
        ELSE UPPER(pipe_state)
    END,
    ad_status,
    CAST(views_2s AS INT),
    -- Device Type Standardization
    CASE 
        WHEN LOWER(device_type) IN ('phone', 'ph0ne', 'mobile') THEN 'mobile'
        WHEN LOWER(device_type) = 'tablet' THEN 'tablet'
        WHEN LOWER(device_type) = 'desktop' THEN 'desktop'
        ELSE 'unknown'
    END
FROM raw_tiktok_ads_hourly
WHERE UPPER(pipe_state) NOT IN ('QRN', 'QUAR', 'ERROR')
  AND ad_id IS NOT NULL;


-- 3. YOUTUBE ADS
INSERT INTO unified_ads (platform, ad_id, timestamp_utc, impressions, clicks, spend, currency, pipeline_status, ad_lifecycle_status, video_views, device_type)
SELECT 
    'youtube',
    ad_id,
    -- handling date + hour
    CAST((segments_date::DATE + (segments_hour || ' hours')::INTERVAL) AS TIMESTAMP),
    CAST(impressions AS INT),
    CAST(clicks AS INT),
    CAST(cost_micros AS NUMERIC) / 1000000.0,
    currency_code,
    CASE 
        WHEN UPPER(pipeline_status) IN ('VAL', 'VALID') THEN 'VALIDATED'
        ELSE UPPER(pipeline_status)
    END,
    primary_status,
    CAST(views AS INT),
    CASE 
        WHEN LOWER(device_category) IN ('mobile', 'mobil') THEN 'mobile'
        WHEN LOWER(device_category) = 'desktop' THEN 'desktop'
        WHEN LOWER(device_category) = 'tablet' THEN 'tablet'
        WHEN LOWER(device_category) = 'tv' THEN 'tv'
        ELSE 'unknown'
    END
FROM raw_youtube_ads_hourly
WHERE UPPER(pipeline_status) NOT IN ('QRN', 'QUAR', 'ERROR')
  AND ad_id IS NOT NULL;

-- ... (Other platforms would go here with NULL device_type or mapped)

-- 4. Currency Conversion (Update Unified Ads in place or use CTE above)
-- Update spend to USD using latest rate (simplification)
-- Ideally join on hour.
UPDATE unified_ads u
SET spend = u.spend / f.rate,
    currency = 'USD'
FROM raw_fx_rates_hourly f
WHERE u.currency = f.quote_currency 
    AND date_trunc('hour', u.timestamp_utc) = date_trunc('hour', CAST(f.fx_hour_utc AS TIMESTAMP))
    AND u.currency != 'USD';
