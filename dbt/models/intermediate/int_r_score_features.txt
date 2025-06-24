WITH constants AS (
    SELECT
        -- Define all US states and territories once
        ARRAY_CONSTRUCT(
            'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
            'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
            'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
        ) AS all_us_states,
        
        -- Define nearby states (bordering Texas)
        ARRAY_CONSTRUCT('OK', 'LA', 'NM', 'AR') AS nearby_states,
        
        -- Define home state
        'TX' AS home_state
),

source AS (
    SELECT * FROM {{ ref('stg_probate_filings_cleaned') }}
),

state_extraction AS (
    SELECT
        source.*,
        constants.*,

        -- Extract property_state with robust regex patterns
        -- Handles formats like "City, ST 12345" and "City ST 12345"
        COALESCE(
            -- Pattern 1: Look for state code after a comma (e.g., "Austin, TX 78701")
            REGEXP_SUBSTR(UPPER(hcad_site_address), ',\\s*([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 2: Look for state code without comma (e.g., "Austin TX 78701")
            REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 3: Look for state code at end with optional zip (e.g., "123 Main St TX")
            REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e'),
            
            -- Default if no pattern matches
            'UNKNOWN'
        ) AS property_state_raw,

        -- Extract mailing_state with robust regex patterns
        -- Handles formats like "City, ST 12345" and "City ST 12345"
        COALESCE(
            -- Pattern 1: Look for state code after a comma (e.g., "Houston, TX 77001")
            REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), ',\\s*([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 2: Look for state code without comma (e.g., "Houston TX 77001")
            REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 3: Look for state code at end with optional zip (e.g., "PO Box 123 TX")
            REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e'),
            
            -- Default if no pattern matches
            'UNKNOWN'
        ) AS mailing_state_raw

    FROM source
    CROSS JOIN constants
)

SELECT
    state_extraction.*,

    -- Validate property_state against US states
    CASE
        WHEN ARRAY_CONTAINS(property_state_raw::VARIANT, all_us_states) THEN property_state_raw
        WHEN property_state_raw = 'UNKNOWN' THEN 'UNKNOWN'
        ELSE 'FOREIGN'
    END AS property_state,

    -- Validate mailing_state against US states
    CASE
        WHEN ARRAY_CONTAINS(mailing_state_raw::VARIANT, all_us_states) THEN mailing_state_raw
        WHEN mailing_state_raw = 'UNKNOWN' THEN 'UNKNOWN'
        ELSE 'FOREIGN'
    END AS mailing_state,

    -- Categorize mailing location type
    CASE
        -- Same state (Texas)
        WHEN mailing_state_raw = home_state THEN 'SAME_STATE'
        
        -- Nearby states (bordering Texas or commonly seen)
        WHEN ARRAY_CONTAINS(mailing_state_raw::VARIANT, nearby_states) THEN 'NEARBY_STATE'
        
        -- Other US states
        WHEN ARRAY_CONTAINS(mailing_state_raw::VARIANT, all_us_states) 
            AND mailing_state_raw != home_state
            AND NOT ARRAY_CONTAINS(mailing_state_raw::VARIANT, nearby_states) 
        THEN 'US_OTHER'
        
        -- Foreign (detected two-letter code that's not a US state)
        WHEN mailing_state_raw != 'UNKNOWN' 
            AND NOT ARRAY_CONTAINS(mailing_state_raw::VARIANT, all_us_states)
        THEN 'FOREIGN'
        
        -- Unknown (no pattern matched)
        ELSE 'UNKNOWN'
    END AS mailing_location_type,

    -- R1: Probate filing ≥ 90 days ago
    CASE
        WHEN "probate_lead_filing_date" IS NOT NULL
            AND DATEDIFF('day', "probate_lead_filing_date", CURRENT_DATE) >= 90
    THEN 15 ELSE 0
    END AS r1_score,

    -- R2: Probate status = 'Open'
    CASE
        WHEN LOWER("probate_lead_status") = 'open' THEN 25 ELSE 0
    END AS r2_score,

    -- R3: Subtype contains 'Affidavit' or 'Administration'
    CASE
        WHEN LOWER("probate_lead_subtype") LIKE ANY ('%affidavit%', '%administration%')
        THEN 15 ELSE 0
    END AS r3_score,

    -- R4: Decedent last-name present
    CASE
        WHEN "probate_lead_decedent_last" IS NOT NULL THEN 10 ELSE 0
    END AS r4_score,

    -- R5: Decedent full-name has ≥ 2 tokens
    CASE
        WHEN LENGTH(TRIM("probate_lead_decedent_first" || ' ' || "probate_lead_decedent_last")) 
            - LENGTH(REPLACE(TRIM("probate_lead_decedent_first" || ' ' || "probate_lead_decedent_last"), ' ', '')) >= 1
        THEN 10 ELSE 0
    END AS r5_score,

    -- R10: Prior years taxes due > 0
    CASE
        WHEN TRY_CAST("prior_years_taxes_due" AS NUMBER) > 0 THEN 25 ELSE 0
    END AS r10_score,

    -- R11: Homestead exemption (negative score)
    CASE
        WHEN LOWER("exemption_code") LIKE '%hs%' THEN -10 ELSE 0
    END AS r11_score,

    -- R12.5: Out-of-state owner scoring based on mailing_location_type
    CASE
        WHEN mailing_location_type = 'NEARBY_STATE' THEN 10
        WHEN mailing_location_type = 'US_OTHER' THEN 15
        WHEN mailing_location_type = 'FOREIGN' THEN 30
        ELSE 0
    END AS r12_5_score,

    -- R13: Owner last name matches decedent last name
    CASE
        WHEN SPLIT_PART(LOWER("hcad_owner_full_name"), ' ', -1) = LOWER("probate_lead_decedent_last") THEN 20 ELSE 0
    END AS r13_score,

    -- R17: Large lot > 7500 sqft
    CASE
        WHEN TRY_CAST(lot_sqft AS NUMBER) > 7500 THEN 5 ELSE 0
    END AS r17_score,

    -- Final score calculation
    (
        r1_score + r2_score + r3_score + r4_score + r5_score +
        r10_score + r11_score + r12_5_score + r13_score + r17_score
    ) AS r_score_acquisition

FROM state_extraction