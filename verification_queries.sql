-- ==========================================
-- MANUAL TEST SUITE: Ad Pipeline Verification
-- Run these queries in the Supabase SQL Editor
-- ==========================================

-- TEST CASE 1: Row Count Verification
-- Requirement: unified_ads rows should roughly match fact_ad_performance rows
-- (Fact table might be slightly less if some IDs were invalid, but usually 1:1)
SELECT 
    (SELECT COUNT(*) FROM unified_ads) as staging_count,
    (SELECT COUNT(*) FROM fact_ad_performance) as fact_count,
    (SELECT COUNT(*) FROM ads_quarantine) as quarantine_count;

-- TEST CASE 2: Quarantine Logic Check
-- Requirement: unified_ads should ONLY have 'VALIDATED', 'PUBLISHED', etc.
-- ads_quarantine should ONLY have 'QUARANTINED', 'ERROR'.
SELECT DISTINCT pipeline_status FROM unified_ads;  -- Should NOT see 'QUARANTINED'
SELECT DISTINCT pipeline_status FROM ads_quarantine; -- Should ONLY see 'QUARANTINED'/'ERROR'

-- TEST CASE 3: Star Schema Integrity (Join Test)
-- Requirement: Joining Fact to Dimensions should return readable names (e.g. 'Meta', 'TikTok')
-- If this returns NULLs for platform_name, something is wrong with the IDs.
SELECT 
    f.ad_id,
    p.platform_name,
    t.timestamp_utc,
    f.spend,
    f.impresions -- Intentional typo in ID? No, checking column exist.
    -- f.impressions
FROM fact_ad_performance f
JOIN dim_platform p ON f.platform_id = p.platform_id
JOIN dim_time t ON f.time_id = t.time_id
LIMIT 10;

-- TEST CASE 4: Analytical Query (Aggregation)
-- Requirement: Can we see total spend per platform? (The Dashboard Query)
SELECT 
    p.platform_name,
    COUNT(f.fact_id) as total_ads,
    SUM(f.impressions) as total_impressions,
    SUM(f.video_views) as total_video_views,
    ROUND(SUM(f.spend), 2) as total_spend_usd
FROM fact_ad_performance f
JOIN dim_platform p ON f.platform_id = p.platform_id
GROUP BY p.platform_name
ORDER BY total_spend_usd DESC;

-- TEST CASE 5: Time Dimension Hierarchy
-- Requirement: Drill down by Year/Month
SELECT 
    t.year,
    t.month,
    SUM(f.clicks) as total_clicks
FROM fact_ad_performance f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month
ORDER BY t.year, t.month;
