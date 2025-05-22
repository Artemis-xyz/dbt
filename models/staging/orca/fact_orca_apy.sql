{{ config(
    materialized="table"
) }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_orca_pools") }}
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
    pool:address::string as id,
    pool:feeRate::float as fees,
    pool:tokenA:symbol::string as tokenA_symbol,
    pool:tokenB:symbol::string as tokenB_symbol,
    pool:tvlUsdc::float as tvl,
    pool:yieldOverTvl::float as yield,
    extraction_date
  from flattened
)
select
    id,
    iff(tokenB_symbol is null, tokenA_symbol, concat(tokenA_symbol, '-', tokenB_symbol)) as name,
    yield * 365 as apy,
    tvl,
    fees / 10000.0 as fees,
    array_construct(tokenA_symbol, tokenB_symbol) as symbol,
    'orca' as protocol,
    'Pool' as type,
    'solana' as chain,
    extraction_date as extraction_timestamp
from extracted