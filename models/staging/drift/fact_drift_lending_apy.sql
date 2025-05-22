{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_drift_lending_market_data") }}
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
    p.symbol,
    p.link,
    extraction_date
  from flattened
  inner join {{ ref("drift_stablecoin_pool_ids") }} p
  on pool:market_name::string = p.name
)

select
  market
  , apy / 100 as apy
  , tvl
  , array_construct(symbol) as symbol
  , 'drift' as protocol
  , 'Lending' as type
  , 'solana' as chain
  , link
  , extraction_date as extraction_timestamp
from extracted