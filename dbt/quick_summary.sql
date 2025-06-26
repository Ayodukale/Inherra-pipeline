SELECT 
    COUNT(*) as total_records,
    ROUND(AVG(r_score_acquisition), 2) as avg_score,
    MAX(r_score_acquisition) as max_score,
    COUNT(CASE WHEN r_score_acquisition > 50 THEN 1 END) as high_value_leads
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features;