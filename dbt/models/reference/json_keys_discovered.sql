-- This model discovers all keys present in the raw JSON data.
-- It serves as a "report" to compare against our official reference table.

{{ config(materialized='table') }}

with raw_source as (
  select raw_record
  from {{ source('probate_raw','PROBATE_FILINGS_ENRICHED') }}
),

unnested_keys as (
  select distinct f.key as column_name
  from raw_source,
       lateral flatten(input => raw_source.raw_record) f
  where f.key is not null
)

select
    column_name
from unnested_keys
order by column_name