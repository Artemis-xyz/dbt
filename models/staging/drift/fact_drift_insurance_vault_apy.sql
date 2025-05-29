{{ config(materialized="table") }}

with base_if as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_drift_spot_market_data") }}
),

flattened_if as (
  select
    value as pool,
    base_if.extraction_date
  from base_if,
  lateral flatten(input => parse_json(base_if.source_json))
),

extracted_if as (
  select
    pool:market_name::string as market,
    pool:staking_apr::float as apy,
    pool:protocol_balance::float as protocol_balance,
    pool:user_balance::float as user_balance,
    p.symbol,
    p.link,
    extraction_date
  from flattened_if
  inner join {{ ref("drift_stablecoin_pool_ids") }} p
    on pool:market_name::string = p.name
)

select
  market
  , apy
  , user_balance + protocol_balance as tvl
  , array_construct(symbol) as symbol
  , 'drift' as protocol
  , 'Insurance Vaults' as type
  , 'solana' as chain
  , link
  , extraction_date as extraction_timestamp
from extracted_if