{{ config(
    materialized="table"
) }}

with flattened as (
  select
    value:"date"::string as date,
    value:"value"::string as value,
    base.extraction_date
  from {{ source("PROD_LANDING", "raw_fedfunds") }} as base,
    lateral flatten(input => parse_json(base.source_json):observations) as value
)

select
  date as timestamp,
  extraction_date,
  'Fed Funds' as name,
  value / 100 as apy,
  null as tvl,
  [] as symbol,
  'fedfunds' as protocol,
  'Fed Fund Rates' as type,
  null as link
from flattened
where extraction_date = (select max(extraction_date) from flattened)
  and date = (select max(date) from flattened where extraction_date = (select max(extraction_date) from flattened))
