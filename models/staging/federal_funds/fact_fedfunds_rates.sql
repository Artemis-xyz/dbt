{{ config(
    materialized="table"
) }}

with latest_base as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_fedfunds") }}
  where extraction_date = (
    select max(extraction_date)
    from {{ source("PROD_LANDING", "raw_fedfunds") }}
  )
),

flattened as (
  select
    value:"date"::string as date,
    value:"value"::string as value,
    latest_base.extraction_date
  from latest_base,
    lateral flatten(input => parse_json(latest_base.source_json):observations) as value
)

select
  date as timestamp,
  extraction_date,
  'Fed Funds Rate' as name,
  value / 100 as apy,
  null as tvl,
  [] as symbol,
  'fed_funds' as protocol,
  'Fed Fund Rates' as type,
  null as chain,
  null as link,
  5.0 as tvl_score
from flattened
where date = (select max(date) from flattened)
