-- =====================================
-- Edge Case Analysis for State Extraction
-- =====================================

-- 1. CHECK FOR ADDRESSES THAT MIGHT STILL BE MISCLASSIFIED
-- =========================================================

-- Look for potential edge cases that our regex might miss
SELECT 
    'Potential Edge Cases' as analysis_type,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_location_type,
    r12_5_score,
    CASE 
        -- Flag addresses that might have issues
        WHEN hcad_mailing_address LIKE '%STREET%' THEN 'Has STREET word'
        WHEN hcad_mailing_address LIKE '%ROAD%' THEN 'Has ROAD word'
        WHEN hcad_mailing_address LIKE '%LANE%' THEN 'Has LANE word'
        WHEN hcad_mailing_address LIKE '%DRIVE%' THEN 'Has DRIVE word'
        WHEN hcad_mailing_address LIKE '%AVENUE%' THEN 'Has AVENUE word'
        WHEN hcad_mailing_address LIKE '%BOULEVARD%' THEN 'Has BOULEVARD word'
        WHEN hcad_mailing_address LIKE '%CIRCLE%' THEN 'Has CIRCLE word'
        WHEN hcad_mailing_address LIKE '%COURT%' THEN 'Has COURT word'
        WHEN hcad_mailing_address LIKE '%PLACE%' THEN 'Has PLACE word'
        WHEN hcad_mailing_address LIKE '%PKWY%' THEN 'Has PKWY'
        WHEN hcad_mailing_address LIKE '%BLVD%' THEN 'Has BLVD'
        WHEN REGEXP_COUNT(hcad_mailing_address, '[A-Z]{2}') > 2 THEN 'Multiple 2-letter combos'
        WHEN hcad_mailing_address NOT LIKE '%TX%' THEN 'No TX in address'
        ELSE 'Standard format'
    END as edge_case_type
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE mailing_location_type = 'FOREIGN'  -- Still classified as foreign
LIMIT 25

UNION ALL

-- 2. CHECK FOR UNKNOWN STATES (our regex failed completely)
-- ========================================================
SELECT 
    'Unknown States' as analysis_type,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_location_type,
    r12_5_score,
    'Regex failed to extract any state' as edge_case_type
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE mailing_state_raw = 'UNKNOWN'
   AND hcad_mailing_address IS NOT NULL
   AND LENGTH(hcad_mailing_address) > 10  -- Not just empty/short addresses
LIMIT 15

UNION ALL

-- 3. CHECK FOR UNLIKELY STATE CODES (might be extracting wrong things)
-- ===================================================================
SELECT 
    'Suspicious State Codes' as analysis_type,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_location_type,
    r12_5_score,
    'Extracted unusual 2-letter combo' as edge_case_type
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE mailing_state_raw NOT IN (
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','FOREIGN','UNKNOWN'
)
LIMIT 15

UNION ALL

-- 4. CHECK FOR ADDRESSES WITH MULTIPLE ZIPS OR WEIRD FORMATS
-- ==========================================================
SELECT 
    'Weird Address Formats' as analysis_type,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_location_type,
    r12_5_score,
    CASE 
        WHEN REGEXP_COUNT(hcad_mailing_address, '\\d{5}') > 1 THEN 'Multiple ZIP codes'
        WHEN hcad_mailing_address LIKE '%APT%' OR hcad_mailing_address LIKE '%UNIT%' THEN 'Has apartment/unit'
        WHEN hcad_mailing_address LIKE '%#%' THEN 'Has # symbol'
        WHEN hcad_mailing_address LIKE '%PO BOX%' OR hcad_mailing_address LIKE '%P.O.%' THEN 'PO Box format'
        WHEN LENGTH(hcad_mailing_address) > 100 THEN 'Very long address'
        WHEN REGEXP_COUNT(hcad_mailing_address, ',') > 2 THEN 'Multiple commas'
        ELSE 'Other weird format'
    END as edge_case_type
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE (
    REGEXP_COUNT(hcad_mailing_address, '\\d{5}') > 1  -- Multiple ZIPs
    OR hcad_mailing_address LIKE '%APT%' 
    OR hcad_mailing_address LIKE '%UNIT%'
    OR hcad_mailing_address LIKE '%#%'
    OR hcad_mailing_address LIKE '%PO BOX%'
    OR hcad_mailing_address LIKE '%P.O.%'
    OR LENGTH(hcad_mailing_address) > 100
    OR REGEXP_COUNT(hcad_mailing_address, ',') > 2
)
LIMIT 20

ORDER BY analysis_type, edge_case_type;