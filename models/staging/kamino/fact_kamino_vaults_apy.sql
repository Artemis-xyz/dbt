{{ config(
    materialized="table"
) }}

with base as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_kamino_vaults") }}
),

flattened as (
  select
    value as vault,
    base.extraction_date
  from base,
  lateral flatten(input => parse_json(base.source_json))
),

extracted as (
  select
    vault:strategy::string as strategy,
    vault:tokenA::string as tokenA,
    vault:tokenB::string as tokenB,
    vault:totalValueLocked::float as total_value_locked,
    vault:kaminoApy:totalApy::float as apr,
    extraction_date
  from flattened
)

select
    strategy as id,
    total_value_locked as tvl,
    (power(1 + (apr / 365), 365) - 1) as apy,
    iff(tokenB is null, tokenA, concat(tokenA, '-', tokenB)) as name,
    array_construct(tokenA, tokenB) as symbol,
    'Pool' as type,
    'kamino' as protocol,
    'solana' as chain,
    extraction_date as extraction_timestamp
from extracted