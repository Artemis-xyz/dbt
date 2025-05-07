{{ config(
    materialized="table"
) }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_raydium_pools") }}
  where extraction_date = (
    select max(extraction_date)
    from {{ source("PROD_LANDING", "raw_raydium_pools") }}
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
    pool:id::string as id,
    pool:mintA:symbol::string as mintA_symbol,
    pool:mintB:symbol::string as mintB_symbol,
    pool:feeRate::float as fees,
    pool:day:apr::float as apr,
    pool:tvl::float as tvl,
    extraction_date
  from flattened
)

select
  id,
  iff(mintB_symbol is null, mintA_symbol, concat(mintA_symbol, '-', mintB_symbol)) as name,
  (power(1 + ((apr / 100) / 365), 365) - 1) as apy,
  tvl,
  fees,
  array_construct(mintA_symbol, mintB_symbol) as symbol,
  'raydium' as protocol,
  'pool' as type,
  extraction_date as extraction_timestamp
from extracted
