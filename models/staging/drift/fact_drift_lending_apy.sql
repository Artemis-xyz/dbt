{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_drift_lending_market_data") }}
  where extraction_date = (
    select max(extraction_date)
    from {{ source("PROD_LANDING", "raw_drift_lending_market_data") }}
  )
),

flattened as (
  select
    value as pool,
    base.extraction_date
  from base,
  lateral flatten(input => parse_json(base.source_json))
),

extracted as (
  select
    pool:market_name::string as market,
    pool:supply_apy::float as apy,
    pool:tvl::float as tvl,
    extraction_date
  from flattened
)

select
  extraction_date as timestamp
  , market
  , apy / 100 as apy
  , tvl
  , 'drift' as protocol
  , 'Lending' as type
  , 'solana' as chain
from extracted