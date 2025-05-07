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
  'Fed Funds' as name,
  value / 100 as apy,
  null as tvl,
  [] as symbol,
  'fedfunds' as protocol,
  'fed fund rates' as type,
  null as link
from flattened
where date = (select max(date) from flattened)
