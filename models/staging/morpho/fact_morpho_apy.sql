{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_morpho_vaults") }}
),

flattened as (
  select
    value as vaults,
    base.extraction_date
    from base,
  lateral flatten(input => parse_json(base.source_json))
),

extracted as (
  select
    vaults:id::string as id,
    vaults:response:name::string as name,
    vaults:response:state:apy::float as apy,
    vaults:response:state:totalAssetsUsd::float as tvl,
    v.chain,
    v.symbol,
    v.link,
    extraction_date
  from flattened
  inner join pc_dbt_db.prod.morpho_stablecoin_vault_ids v
  on v.id = vaults:id::string
)

select
  id
  , name
  , apy
  , tvl
  , array_construct(symbol) as symbol
  , 'morpho' as protocol
  , 'Vault' as type
  , case when chain = 1 then 'ethereum' end as chain
  , link
  , extraction_date as extraction_timestamp
from extracted