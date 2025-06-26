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
        -- Handles formats like "City, ST 12345" and "City ST 12345-1234"
        COALESCE(
            -- Pattern 1: Look for state code after a comma before ZIP (most reliable)
            REGEXP_SUBSTR(UPPER(hcad_site_address), ',\\s*([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 2: Look for state code before ZIP, then filter out street abbreviations
            CASE 
                WHEN REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e') 
                     NOT IN ('ST', 'RD', 'DR', 'LN', 'AVE', 'CT', 'PL', 'WAY', 'CIR')
                THEN REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e')
            END,
            
            -- Pattern 3: Look for state code at very end without ZIP (last resort)
            CASE 
                WHEN REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e')
                     NOT IN ('ST', 'RD', 'DR', 'LN', 'AVE', 'CT', 'PL', 'WAY', 'CIR')
                THEN REGEXP_SUBSTR(UPPER(hcad_site_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e')
            END,
            
            -- Default if no pattern matches
            'UNKNOWN'
        ) AS property_state_raw,

        -- Extract mailing_state with robust regex patterns  
        -- Handles formats like "City, ST 12345" and "City ST 12345-1234"
        COALESCE(
            -- Pattern 1: Look for state code after a comma before ZIP (most reliable)
            REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), ',\\s*([A-Z]{2})\\s+\\d{5}', 1, 1, 'e'),
            
            -- Pattern 2: Look for state code before ZIP, then filter out street abbreviations
            CASE 
                WHEN REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e') 
                     NOT IN ('ST', 'RD', 'DR', 'LN', 'AVE', 'CT', 'PL', 'WAY', 'CIR')
                THEN REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s+\\d{5}', 1, 1, 'e')
            END,
            
            -- Pattern 3: Look for state code at very end without ZIP (last resort)
            CASE 
                WHEN REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e')
                     NOT IN ('ST', 'RD', 'DR', 'LN', 'AVE', 'CT', 'PL', 'WAY', 'CIR')
                THEN REGEXP_SUBSTR(UPPER(hctax_owner_mailing_address), '\\s+([A-Z]{2})\\s*$', 1, 1, 'e')
            END,
            
            -- Default if no pattern matches
            'UNKNOWN'
        ) AS mailing_state_raw

    FROM source
    CROSS JOIN constants
),

rules_calculation AS (
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
            WHEN probate_lead_filing_date IS NOT NULL
                AND DATEDIFF('day', probate_lead_filing_date, CURRENT_DATE) >= 90
            THEN OBJECT_CONSTRUCT(
                'score', 15,
                'tag', 'PROBATE_TIMING',
                'reasoning', 'PROBATE_TIMING: Probate case filed ≥90 days ago; time window suggests administrative delays or lack of heirs.',
                'confidence', 0.85,
                'confidence_reason', 'Court records are reliable and updated regularly.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_1_output,

        -- R2: Probate status = 'Open'
        CASE
            WHEN LOWER(probate_lead_status) = 'open'
            THEN OBJECT_CONSTRUCT(
                'score', 25,
                'tag', 'PROBATE_STATUS',
                'reasoning', 'PROBATE_STATUS: Probate case is still open, suggesting the estate is unresolved.',
                'confidence', 0.85,
                'confidence_reason', 'Court records are reliable and updated regularly.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_2_output,

        -- R3: Subtype contains 'Affidavit' or 'Administration'
        CASE
            WHEN LOWER(probate_lead_subtype) LIKE ANY ('%affidavit%', '%administration%')
            THEN OBJECT_CONSTRUCT(
                'score', 15,
                'tag', 'PROBATE_TYPE',
                'reasoning', 'PROBATE_TYPE: Probate subtype suggests informal or non-standard process (Affidavit or Administration).',
                'confidence', 0.85,
                'confidence_reason', 'Court records are reliable and updated regularly.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_3_output,

        -- R4: Decedent last-name present
        CASE
            WHEN probate_lead_decedent_last IS NOT NULL
            THEN OBJECT_CONSTRUCT(
                'score', 10,
                'tag', 'NAME_SIGNAL',
                'reasoning', 'NAME_SIGNAL: Decedent\'s last name is present, aiding name match inference.',
                'confidence', 0.75,
                'confidence_reason', 'Name data is useful but can have variations or common matches.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_4_output,

        -- R5: Decedent full-name has ≥ 2 tokens
        CASE
            WHEN LENGTH(TRIM(probate_lead_decedent_first || ' ' || probate_lead_decedent_last)) 
                - LENGTH(REPLACE(TRIM(probate_lead_decedent_first || ' ' || probate_lead_decedent_last), ' ', '')) >= 1
            THEN OBJECT_CONSTRUCT(
                'score', 10,
                'tag', 'NAME_COMPLEXITY',
                'reasoning', 'NAME_COMPLEXITY: Decedent\'s full name has 2+ tokens, improving match confidence.',
                'confidence', 0.75,
                'confidence_reason', 'Name data is useful but can have variations or common matches.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_5_output,

        -- R10: Prior years taxes due > 0
        CASE
            WHEN TRY_CAST(prior_years_taxes_due AS NUMBER) > 0
            THEN OBJECT_CONSTRUCT(
                'score', 25,
                'tag', 'TAX_DISTRESS',
                'reasoning', 'TAX_DISTRESS: Delinquent on prior years taxes.',
                'confidence', 0.9,
                'confidence_reason', 'Tax records are official government data with high accuracy.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_10_output,

        -- R11: Homestead exemption (negative score)
        CASE
            WHEN LOWER(exemption_code) LIKE '%hs%'
            THEN OBJECT_CONSTRUCT(
                'score', -10,
                'tag', 'OWNER_OCCUPIED',
                'reasoning', 'OWNER_OCCUPIED: Homestead exemption present; likely owner-occupied and less motivated.',
                'confidence', 0.8,
                'confidence_reason', 'Homestead data is accurate but may lag changes in residency.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_11_output,

        -- R12.5: Out-of-state owner scoring based on mailing_location_type
        CASE
            WHEN mailing_location_type = 'NEARBY_STATE'
            THEN OBJECT_CONSTRUCT(
                'score', 10,
                'tag', 'OWNER_LOCATION',
                'reasoning', 'OWNER_LOCATION: Owner\'s mailing address is in a nearby state (moderate distance from property).',
                'confidence', 0.78,
                'confidence_reason', 'Mailing addresses are generally accurate but don\'t always indicate true residence.'
            )
            WHEN mailing_location_type = 'US_OTHER'
            THEN OBJECT_CONSTRUCT(
                'score', 15,
                'tag', 'OWNER_LOCATION',
                'reasoning', 'OWNER_LOCATION: Owner\'s mailing address is in a distant U.S. state (higher detachment).',
                'confidence', 0.78,
                'confidence_reason', 'Mailing addresses are generally accurate but don\'t always indicate true residence.'
            )
            WHEN mailing_location_type = 'FOREIGN'
            THEN OBJECT_CONSTRUCT(
                'score', 30,
                'tag', 'OWNER_LOCATION',
                'reasoning', 'OWNER_LOCATION: Owner\'s mailing address is international (highest detachment).',
                'confidence', 0.78,
                'confidence_reason', 'Mailing addresses are generally accurate but don\'t always indicate true residence.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_12_5_output,

        -- R13: Owner last name matches decedent last name
        CASE
            WHEN SPLIT_PART(LOWER(hcad_owner_full_name), ' ', -1) = LOWER(probate_lead_decedent_last)
            THEN OBJECT_CONSTRUCT(
                'score', 20,
                'tag', 'NAME_MATCH',
                'reasoning', 'NAME_MATCH: Owner\'s last name matches decedent\'s last name.',
                'confidence', 0.75,
                'confidence_reason', 'Name matching is helpful but can have false positives with common surnames.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_13_output,

        -- R14: Year built < 1960 (requires extracting from JSON)
        CASE
            WHEN TRY_TO_NUMBER(
                PARSE_JSON(hcad_building_data_json):year_built::STRING
            ) < 1960
            THEN OBJECT_CONSTRUCT(
                'score', 10,
                'tag', 'AGE_SIGNAL',
                'reasoning', 'AGE_SIGNAL: Built before 1960; older properties may have deferred maintenance.',
                'confidence', 0.65,
                'confidence_reason', 'Property age data is reliable, but actual condition varies widely.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_14_output,

        -- R15: Poor condition or Grade C/C-
        CASE
            WHEN UPPER(hcad_physical_condition) IN ('POOR', 'VERY POOR', 'UNSOUND')
                OR UPPER(hcad_grade_adjustment) IN ('C', 'C-', 'D', 'D-', 'E', 'F')
            THEN OBJECT_CONSTRUCT(
                'score', 15,
                'tag', 'CONDITION',
                'reasoning', 'CONDITION: Assessor grade is C or lower, suggesting physical or cosmetic issues.',
                'confidence', 0.65,
                'confidence_reason', 'Condition assessments are based on external observation and may miss recent updates.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_15_output,

        -- R16: Land-to-total value ≥ 0.40
        CASE
            WHEN TRY_TO_NUMBER(hcad_land_market_value) > 0 
                AND TRY_TO_NUMBER(hcad_market_value_detail) > 0
                AND (TRY_TO_NUMBER(hcad_land_market_value) / NULLIF(TRY_TO_NUMBER(hcad_market_value_detail), 0)) >= 0.40
            THEN OBJECT_CONSTRUCT(
                'score', 10,
                'tag', 'LAND_RATIO',
                'reasoning', 'LAND_RATIO: Land value is ≥ 40% of total, indicating potential redevelopment value.',
                'confidence', 0.7,
                'confidence_reason', 'Land valuations are stable but can be outdated in rapidly changing markets.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_16_output,

        -- R16.5: Garage present (garage_area > 0)
        CASE
            WHEN TRY_TO_NUMBER(hcad_garage_sqft) > 0
            THEN OBJECT_CONSTRUCT(
                'score', 2,
                'tag', 'GARAGE',
                'reasoning', 'GARAGE: Garage present — minor marketability upgrade.',
                'confidence', 0.6,
                'confidence_reason', 'Garage data is sometimes inaccurate, especially for converted or detached structures.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_16_5_output,

        -- R17: Large lot > 7500 sqft
        CASE
            WHEN TRY_CAST(hcad_lot_sqft_total AS NUMBER) > 7500
            THEN OBJECT_CONSTRUCT(
                'score', 5,
                'tag', 'LOT_SIZE',
                'reasoning', 'LOT_SIZE: Lot size is over 7500 sqft, offering potential for expansion or appeal.',
                'confidence', 0.7,
                'confidence_reason', 'Lot size data is generally accurate but boundary disputes can occur.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_17_output,

        -- R17.5: Multi-segment lot (land_line_count > 1)
        CASE
            WHEN TRY_TO_NUMBER(hcad_land_line_count) > 1
            THEN OBJECT_CONSTRUCT(
                'score', 5,
                'tag', 'LOT_SEGMENTS',
                'reasoning', 'LOT_SEGMENTS: Multiple land segments suggest non-standard parcel, possible value or complexity.',
                'confidence', 0.7,
                'confidence_reason', 'Lot segmentation data is stable but can reflect outdated subdivisions.'
            )
            ELSE OBJECT_CONSTRUCT(
                'score', 0,
                'tag', NULL,
                'reasoning', NULL,
                'confidence', NULL,
                'confidence_reason', NULL
            )
        END AS rule_17_5_output,

        -- R100: Deed Transfer Status
        CASE
            -- Condition 1: Finalizing deed (Warranty/Special) - property likely sold
            WHEN UPPER(rp_instrument_type) IN ('W/D', 'DEED', 'SPECIAL WARRANTY DEED')
            THEN OBJECT_CONSTRUCT(
                'score', -20,
                'tag', 'DEED_SALE',
                'reasoning', 'DEED_SALE: A finalizing deed (Warranty/Special) was filed, indicating the property has likely been sold.',
                'confidence', 0.92,
                'confidence_reason', 'High confidence; these are clear indicators of a finalized sale.'
            )
            
            -- Condition 2: Executor's Deed - heir has taken title
            WHEN UPPER(rp_instrument_type) = 'EXECUTOR\'S DEED'
            THEN OBJECT_CONSTRUCT(
                'score', 15,
                'tag', 'DEED_HEIR_XFER',
                'reasoning', 'DEED_HEIR_XFER: An Executor\'s Deed was filed, indicating an heir has formally taken title.',
                'confidence', 0.80,
                'confidence_reason', 'Strong confidence, but a small percentage can be for other legal reasons.'
            )
            
            -- Condition 3: Informal/intra-family transfer
            WHEN UPPER(rp_instrument_type) IN ('AFFT', 'QUITCLAIM DEED', 'GIFT DEED')
            THEN OBJECT_CONSTRUCT(
                'score', 20,
                'tag', 'DEED_INFORMAL_XFER',
                'reasoning', 'DEED_INFORMAL_XFER: An informal or intra-family transfer (Affidavit/Quitclaim) was filed.',
                'confidence', 0.72,
                'confidence_reason', 'Moderate confidence; high variability in motivation for these "soft transfers".'
            )
            
            -- Condition 4: Trustee's Deed - foreclosure/non-probate
            WHEN UPPER(rp_instrument_type) = 'TRUSTEE\'S DEED'
            THEN OBJECT_CONSTRUCT(
                'score', 0,
                'tag', 'DEED_NON_PROBATE',
                'reasoning', 'DEED_NON_PROBATE: A foreclosure or trustee-related deed was found.',
                'confidence', 0.85,
                'confidence_reason', 'High confidence this is a non-probate event.'
            )
            
            -- Condition 5: No finalizing deed found
            ELSE OBJECT_CONSTRUCT(
                'score', 30,
                'tag', 'DEED_NONE_FOUND',
                'reasoning', 'DEED_NONE_FOUND: No finalizing transfer deed was found, suggesting title is still with the estate.',
                'confidence', 0.65,
                'confidence_reason', 'Moderate confidence; absence of a record is a strong signal, but could also be a data lag.'
            )
        END AS rule_100_output

    FROM state_extraction
)

SELECT
    rules_calculation.*,
    
    -- Final score calculation - sum all scores
    (
        rule_1_output:score::INT + 
        rule_2_output:score::INT + 
        rule_3_output:score::INT + 
        rule_4_output:score::INT + 
        rule_5_output:score::INT +
        rule_10_output:score::INT + 
        rule_11_output:score::INT + 
        rule_12_5_output:score::INT + 
        rule_13_output:score::INT + 
        rule_14_output:score::INT + 
        rule_15_output:score::INT + 
        rule_16_output:score::INT + 
        rule_16_5_output:score::INT + 
        rule_17_output:score::INT + 
        rule_17_5_output:score::INT +
        rule_100_output:score::INT
    ) AS r_score_acquisition,

    -- Build the reasoning array - collect all non-null reasoning strings
    ARRAY_CONSTRUCT_COMPACT(
        rule_1_output:reasoning::STRING,
        rule_2_output:reasoning::STRING,
        rule_3_output:reasoning::STRING,
        rule_4_output:reasoning::STRING,
        rule_5_output:reasoning::STRING,
        rule_10_output:reasoning::STRING,
        rule_11_output:reasoning::STRING,
        rule_12_5_output:reasoning::STRING,
        rule_13_output:reasoning::STRING,
        rule_14_output:reasoning::STRING,
        rule_15_output:reasoning::STRING,
        rule_16_output:reasoning::STRING,
        rule_16_5_output:reasoning::STRING,
        rule_17_output:reasoning::STRING,
        rule_17_5_output:reasoning::STRING,
        rule_100_output:reasoning::STRING
    ) AS reasoning_array,

    -- Build the confidence summary array - create structs for each fired rule
    ARRAY_CONSTRUCT_COMPACT(
        IFF(rule_1_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_1_output:tag::STRING, 
                'confidence', rule_1_output:confidence::FLOAT, 
                'reason', rule_1_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_2_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_2_output:tag::STRING, 
                'confidence', rule_2_output:confidence::FLOAT, 
                'reason', rule_2_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_3_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_3_output:tag::STRING, 
                'confidence', rule_3_output:confidence::FLOAT, 
                'reason', rule_3_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_4_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_4_output:tag::STRING, 
                'confidence', rule_4_output:confidence::FLOAT, 
                'reason', rule_4_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_5_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_5_output:tag::STRING, 
                'confidence', rule_5_output:confidence::FLOAT, 
                'reason', rule_5_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_10_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_10_output:tag::STRING, 
                'confidence', rule_10_output:confidence::FLOAT, 
                'reason', rule_10_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_11_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_11_output:tag::STRING, 
                'confidence', rule_11_output:confidence::FLOAT, 
                'reason', rule_11_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_12_5_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_12_5_output:tag::STRING, 
                'confidence', rule_12_5_output:confidence::FLOAT, 
                'reason', rule_12_5_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_13_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_13_output:tag::STRING, 
                'confidence', rule_13_output:confidence::FLOAT, 
                'reason', rule_13_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_14_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_14_output:tag::STRING, 
                'confidence', rule_14_output:confidence::FLOAT, 
                'reason', rule_14_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_15_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_15_output:tag::STRING, 
                'confidence', rule_15_output:confidence::FLOAT, 
                'reason', rule_15_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_16_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_16_output:tag::STRING, 
                'confidence', rule_16_output:confidence::FLOAT, 
                'reason', rule_16_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_16_5_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_16_5_output:tag::STRING, 
                'confidence', rule_16_5_output:confidence::FLOAT, 
                'reason', rule_16_5_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_17_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_17_output:tag::STRING, 
                'confidence', rule_17_output:confidence::FLOAT, 
                'reason', rule_17_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_17_5_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_17_5_output:tag::STRING, 
                'confidence', rule_17_5_output:confidence::FLOAT, 
                'reason', rule_17_5_output:confidence_reason::STRING
            ), 
            NULL),
        IFF(rule_100_output:score::INT != 0, 
            OBJECT_CONSTRUCT(
                'tag', rule_100_output:tag::STRING, 
                'confidence', rule_100_output:confidence::FLOAT, 
                'reason', rule_100_output:confidence_reason::STRING
            ), 
            NULL)
    ) AS confidence_summary_array,

    -- Data contradiction detection
    CASE
        WHEN (
            -- Contradiction 1: Homestead exemption exists AND owner is absentee
            (rule_11_output:score::INT < 0 AND mailing_location_type != 'SAME_STATE')
            OR
            -- Contradiction 2: Property in poor condition AND high grade
            (rule_15_output:score::INT > 0 AND UPPER(hcad_grade_adjustment) IN ('A', 'B', 'A+', 'B+'))
            OR
            -- Contradiction 3: No living area AND bedrooms present
            (TRY_TO_NUMBER(hcad_total_base_sqft) = 0 AND TRY_TO_NUMBER(hcad_bedrooms) > 0)
            OR
            -- Contradiction 4: Large lot but zero land value
            (TRY_CAST(hcad_lot_sqft_total AS NUMBER) > 7500 AND TRY_TO_NUMBER(hcad_land_market_value) = 0)
            OR
            -- Contradiction 5: Garage exists but zero garage sqft
            (TRY_TO_NUMBER(hcad_garage_sqft) = 0 AND 
             (LOWER(hcad_exterior_wall) LIKE '%garage%' OR LOWER(hcad_foundation_type) LIKE '%garage%'))
        )
        THEN TRUE
        ELSE FALSE
    END AS has_data_contradiction

FROM rules_calculation