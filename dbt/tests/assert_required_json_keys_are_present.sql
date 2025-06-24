-- The rule for a dbt test is simple:
-- If this query returns 0 rows, the test PASSES.
-- If this query returns 1 or more rows, the test FAILS.

-- This query is designed to find all "bad" rows that are missing our required keys.

SELECT *
FROM {{ source('probate_raw', 'PROBATE_FILINGS_ENRICHED') }}
WHERE 
  -- Check if the key is missing from the JSON OR if its value is an empty string
  (RAW_RECORD:probate_lead_case_number::STRING IS NULL OR RAW_RECORD:probate_lead_case_number::STRING = '')
  OR 
  (RAW_RECORD:hcad_account::STRING IS NULL OR RAW_RECORD:hcad_account::STRING = '')