-- modeling.sql
-- Goal: Populate Star Schema from unified_ads

-- 1. Populate/Update Dimensions

-- DIM_PLATFORM
INSERT INTO dim_platform (platform_name)
SELECT DISTINCT platform FROM unified_ads
ON CONFLICT (platform_name) DO NOTHING;

-- DIM_DEVICE
INSERT INTO dim_device (device_type)
SELECT DISTINCT device_type FROM unified_ads WHERE device_type IS NOT NULL
ON CONFLICT (device_type) DO NOTHING;

-- DIM_AD_STATUS
INSERT INTO dim_ad_status (lifecycle_status)
SELECT DISTINCT ad_lifecycle_status FROM unified_ads WHERE ad_lifecycle_status IS NOT NULL
ON CONFLICT (lifecycle_status) DO NOTHING;

-- DIM_TIME
-- Generate time dimension records from unified_ads timestamps
INSERT INTO dim_time (timestamp_utc, year, month, day, hour, day_of_week)
SELECT DISTINCT 
    timestamp_utc,
    EXTRACT(YEAR FROM timestamp_utc),
    EXTRACT(MONTH FROM timestamp_utc),
    EXTRACT(DAY FROM timestamp_utc),
    EXTRACT(HOUR FROM timestamp_utc),
    EXTRACT(DOW FROM timestamp_utc)
FROM unified_ads
ON CONFLICT (timestamp_utc) DO NOTHING;

-- DIM_ACCOUNT (Dummy for now as logic was simple)
INSERT INTO dim_account (source_account_id, account_name) 
VALUES ('UNKNOWN', 'Default Account') 
ON CONFLICT (source_account_id) DO NOTHING;


-- 2. Populate Fact Table
-- Clear existing facts if fully reloading or use incremental logic.
-- For ELT full run, usually we might truncate or delete overlapping partitions.
-- Here we TRUNCATE for simplicity as per python script behavior.
TRUNCATE TABLE fact_ad_performance RESTART IDENTITY CASCADE;

INSERT INTO fact_ad_performance 
(ad_id, platform_id, time_id, account_id, status_id, device_id, impressions, clicks, spend, currency_code, pipeline_status, video_views)
SELECT 
    u.ad_id,
    dp.platform_id,
    dt.time_id,
    da.account_id,
    ds.status_id,
    dd.device_id,
    u.impressions,
    u.clicks,
    u.spend,
    u.currency,
    u.pipeline_status,
    u.video_views
FROM unified_ads u
JOIN dim_platform dp ON u.platform = dp.platform_name
JOIN dim_time dt ON u.timestamp_utc = dt.timestamp_utc
JOIN dim_account da ON da.source_account_id = 'UNKNOWN' -- default linkage
LEFT JOIN dim_ad_status ds ON u.ad_lifecycle_status = ds.lifecycle_status
LEFT JOIN dim_device dd ON u.device_type = dd.device_type;
