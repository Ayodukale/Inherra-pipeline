-- =====================================
-- R-Score Analysis Queries
-- =====================================
-- Run these queries in Snowflake to explore your int_r_score_features results

-- 1. BASIC SCORE DISTRIBUTION & SUMMARY STATS
-- ===========================================
SELECT 
    COUNT(*) as total_records,
    ROUND(AVG(r_score_acquisition), 2) as avg_score,
    ROUND(STDDEV(r_score_acquisition), 2) as score_stddev,
    MIN(r_score_acquisition) as min_score,
    MAX(r_score_acquisition) as max_score,
    ROUND(MEDIAN(r_score_acquisition), 2) as median_score,
    COUNT(CASE WHEN r_score_acquisition > 50 THEN 1 END) as high_scores_50_plus,
    COUNT(CASE WHEN r_score_acquisition > 75 THEN 1 END) as very_high_scores_75_plus,
    COUNT(CASE WHEN has_data_contradiction THEN 1 END) as records_with_contradictions
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features;

-- 2. SCORE DISTRIBUTION BY RANGES
-- ===============================
SELECT 
    CASE 
        WHEN r_score_acquisition < 0 THEN 'Negative (< 0)'
        WHEN r_score_acquisition = 0 THEN 'Zero (0)'
        WHEN r_score_acquisition BETWEEN 1 AND 25 THEN 'Low (1-25)'
        WHEN r_score_acquisition BETWEEN 26 AND 50 THEN 'Medium (26-50)'
        WHEN r_score_acquisition BETWEEN 51 AND 75 THEN 'High (51-75)'
        WHEN r_score_acquisition > 75 THEN 'Very High (75+)'
    END as score_range,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
GROUP BY score_range
ORDER BY MIN(r_score_acquisition);

-- 3. TOP 20 SCORING PROPERTIES WITH REASONING
-- ==========================================
SELECT 
    probate_lead_case_number,
    hcad_account,
    hcad_owner_full_name,
    probate_lead_decedent_last,
    r_score_acquisition,
    reasoning_array,
    property_state,
    mailing_location_type,
    has_data_contradiction
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE r_score_acquisition IS NOT NULL
ORDER BY r_score_acquisition DESC
LIMIT 20;

-- 4. BOTTOM 10 SCORING PROPERTIES (Lowest/Negative)
-- ================================================
SELECT 
    probate_lead_case_number,
    hcad_account,
    hcad_owner_full_name,
    probate_lead_decedent_last,
    r_score_acquisition,
    reasoning_array,
    property_state,
    mailing_location_type,
    has_data_contradiction
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE r_score_acquisition IS NOT NULL
ORDER BY r_score_acquisition ASC
LIMIT 10;

-- 5. RULE EFFECTIVENESS ANALYSIS
-- =============================
SELECT 
    'R1 - Probate Old (90+ days)' as rule_name,
    COUNT(CASE WHEN r1_score > 0 THEN 1 END) as times_fired,
    ROUND(COUNT(CASE WHEN r1_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as fire_rate_pct,
    AVG(CASE WHEN r1_score > 0 THEN r1_score END) as avg_score_when_fired
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features

UNION ALL SELECT 'R2 - Probate Still Open', COUNT(CASE WHEN r2_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r2_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r2_score > 0 THEN r2_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R3 - Basic Probate Type', COUNT(CASE WHEN r3_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r3_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r3_score > 0 THEN r3_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R4 - Decedent Last Name Present', COUNT(CASE WHEN r4_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r4_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r4_score > 0 THEN r4_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R5 - Valid Full Name', COUNT(CASE WHEN r5_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r5_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r5_score > 0 THEN r5_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R10 - Tax Delinquent', COUNT(CASE WHEN r10_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r10_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r10_score > 0 THEN r10_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R11 - Homestead (Negative)', COUNT(CASE WHEN r11_score < 0 THEN 1 END), ROUND(COUNT(CASE WHEN r11_score < 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r11_score < 0 THEN r11_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R12.5 - Out of State Owner', COUNT(CASE WHEN r12_5_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r12_5_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r12_5_score > 0 THEN r12_5_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R13 - Owner Matches Decedent', COUNT(CASE WHEN r13_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r13_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r13_score > 0 THEN r13_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R14 - Built Before 1960', COUNT(CASE WHEN r14_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r14_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r14_score > 0 THEN r14_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R15 - Poor Condition', COUNT(CASE WHEN r15_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r15_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r15_score > 0 THEN r15_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R16 - High Land Value Ratio', COUNT(CASE WHEN r16_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r16_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r16_score > 0 THEN r16_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R16.5 - Has Garage', COUNT(CASE WHEN r16_5_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r16_5_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r16_5_score > 0 THEN r16_5_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R17 - Large Lot', COUNT(CASE WHEN r17_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r17_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r17_score > 0 THEN r17_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
UNION ALL SELECT 'R17.5 - Multi-Segment Lot', COUNT(CASE WHEN r17_5_score > 0 THEN 1 END), ROUND(COUNT(CASE WHEN r17_5_score > 0 THEN 1 END) * 100.0 / COUNT(*), 2), AVG(CASE WHEN r17_5_score > 0 THEN r17_5_score END) FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features

ORDER BY times_fired DESC;

-- 6. DATA CONTRADICTION ANALYSIS
-- ==============================
SELECT 
    'Records with Contradictions' as metric,
    COUNT(CASE WHEN has_data_contradiction THEN 1 END) as count,
    ROUND(COUNT(CASE WHEN has_data_contradiction THEN 1 END) * 100.0 / COUNT(*), 2) as percentage
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features

UNION ALL

SELECT 
    'Records without Contradictions',
    COUNT(CASE WHEN NOT has_data_contradiction THEN 1 END),
    ROUND(COUNT(CASE WHEN NOT has_data_contradiction THEN 1 END) * 100.0 / COUNT(*), 2)
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features;

-- 7. PROPERTIES WITH DATA CONTRADICTIONS
-- =====================================
SELECT 
    probate_lead_case_number,
    hcad_account,
    hcad_owner_full_name,
    r_score_acquisition,
    reasoning_array,
    -- Individual contradiction checks (you'd need to reconstruct the logic to see which fired)
    CASE WHEN r11_score < 0 AND mailing_location_type != 'SAME_STATE' THEN 'Homestead + Absentee Owner' END as contradiction_1,
    CASE WHEN r15_score > 0 AND hcad_grade_adjustment IN ('A', 'B', 'A+', 'B+') THEN 'Poor Condition + High Grade' END as contradiction_2
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE has_data_contradiction = TRUE
LIMIT 20;

-- 8. GEOGRAPHIC SCORING PATTERNS
-- ==============================
SELECT 
    property_state,
    mailing_location_type,
    COUNT(*) as property_count,
    ROUND(AVG(r_score_acquisition), 2) as avg_score,
    COUNT(CASE WHEN r_score_acquisition > 50 THEN 1 END) as high_scores,
    ROUND(COUNT(CASE WHEN r_score_acquisition > 50 THEN 1 END) * 100.0 / COUNT(*), 2) as high_score_pct
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE property_state IS NOT NULL AND mailing_location_type IS NOT NULL
GROUP BY property_state, mailing_location_type
HAVING COUNT(*) >= 5  -- Only show combinations with at least 5 properties
ORDER BY avg_score DESC;

-- 9. MOST COMMON REASONING COMBINATIONS
-- ====================================
SELECT 
    reasoning_array,
    COUNT(*) as frequency,
    ROUND(AVG(r_score_acquisition), 2) as avg_score_for_this_reasoning,
    MIN(r_score_acquisition) as min_score,
    MAX(r_score_acquisition) as max_score
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE ARRAY_SIZE(reasoning_array) > 0  -- Only records with actual reasoning
GROUP BY reasoning_array
HAVING COUNT(*) >= 2  -- Only show reasoning that appears at least twice
ORDER BY frequency DESC
LIMIT 25;

-- 10. QUICK SAMPLE OF YOUR DATA
-- =============================
SELECT 
    probate_lead_case_number,
    hcad_account,
    hcad_owner_full_name,
    probate_lead_decedent_last || ', ' || probate_lead_decedent_first as decedent_name,
    r_score_acquisition,
    property_state,
    mailing_location_type,
    has_data_contradiction,
    reasoning_array[0] as top_reason  -- First reason in the array
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
LIMIT 10;