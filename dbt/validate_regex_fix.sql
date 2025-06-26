-- =====================================
-- Validate Regex Fix Results
-- =====================================

-- 1. Check the specific addresses that were previously misclassified
SELECT 
    'Specific Address Check' as query_type,
    probate_lead_case_number,
    hcad_owner_full_name,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_state,
    mailing_location_type,
    r12_5_score
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE hcad_owner_full_name IN ('WOODRING MARK D', 'RESENDIZPORRAS IRIS', 'ALLGAIER LEX')
   OR hcad_mailing_address LIKE '%TRICHELLE ST PASADENA TX%'
   OR hcad_mailing_address LIKE '%PLEASANT VALLEY RD HOUSTON TX%'
   OR hcad_mailing_address LIKE '%W 21ST ST HOUSTON TX%'

UNION ALL

-- 2. Show distribution of mailing location types (should have fewer FOREIGN now)
SELECT 
    'Location Type Distribution' as query_type,
    mailing_location_type as probate_lead_case_number,
    CAST(COUNT(*) AS VARCHAR) as hcad_owner_full_name,
    CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS VARCHAR) || '%' as hcad_mailing_address,
    NULL as mailing_state_raw,
    NULL as mailing_state,
    NULL as mailing_location_type,
    NULL as r12_5_score
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
GROUP BY mailing_location_type

UNION ALL

-- 3. Show remaining FOREIGN addresses (should be actual foreign now)
SELECT 
    'Remaining Foreign Addresses' as query_type,
    probate_lead_case_number,
    hcad_owner_full_name,
    hcad_mailing_address,
    mailing_state_raw,
    mailing_state,
    mailing_location_type,
    r12_5_score
FROM RAW_DATA_DB.dbt_aodukale.int_r_score_features
WHERE mailing_location_type = 'FOREIGN'
LIMIT 10

ORDER BY query_type, probate_lead_case_number;