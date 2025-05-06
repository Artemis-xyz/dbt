{{ config(
    materialized="table"
) }}

with base as (
  select
    parse_json(source_json) as json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_fedfunds") }}
),

flattened as (
  select
    value:"date"::string as date,
    value:"value"::string as value,
    base.extraction_date
  from base,
    lateral flatten(input => base.json:observations)
),

latest_date as (
    select
        max(date) as max_date,
        max(extraction_date) as max_extraction_date
    from flattened
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
join latest_date
on flattened.date = latest_date.max_date
    and flattened.extraction_date = latest_date.max_extraction_date
