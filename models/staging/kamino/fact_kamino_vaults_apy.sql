{{ config(
    materialized="incremental",
    unique_key=[
        'id',
        'extraction_timestamp',
    ],
) }}

with base as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_kamino_vaults") }}
  {% if is_incremental() %}
    where extraction_date > (
      select dateadd('day', -1, max(extraction_timestamp)) from {{ this }}
    )
  {% endif %}
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
    vault:totalValueLocked::float as tvl,
    vault:kaminoApy:totalApy::float as apr,
    extraction_date,
    v.link as link
  from flattened
  inner join {{ ref("kamino_stablecoin_vault_ids") }} v
  on vault:strategy::string = v.id
)

select
    strategy as id,
    iff(tokenB is null, tokenA, concat(tokenA, '-', tokenB)) as name,
    (power(1 + (apr / 365), 365) - 1) as apy,
    tvl,
    array_construct(tokenA, tokenB) as symbol,
    'kamino' as protocol,
    'Pool' as type,
    'solana' as chain,
    link,
    extraction_date as extraction_timestamp
from extracted