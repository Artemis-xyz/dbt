{{ config(
    materialized="table"
) }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_vaults_fyi_data") }}
  where extraction_date = (
    select max(extraction_date)
    from {{ source("PROD_LANDING", "raw_vaults_fyi_data") }}
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
    pool:address::string as id,
    pool:apy:"1day":total::float as apy,
    pool:network:name::string as chain,
    pool:protocol:name::string as protocol,
    pool:name::string as name,
    pool:lendUrl::string as source,
    pool:protocolVaultUrl::string as vault_url,
    pool:tvl:usd::float as tvl,
    pool:tags[0]::string as type,
    extraction_date
  from flattened
)
select
    id,
    name,
    apy,
    tvl,
    protocol,
    type,
    coalesce(source, vault_url) as link,
    case when chain = 'mainnet' then 'ethereum' else chain end as chain,
    extraction_date as extraction_timestamp
from extracted