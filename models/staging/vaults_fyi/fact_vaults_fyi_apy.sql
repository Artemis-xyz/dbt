{{ config(
    materialized="table"
) }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
  from {{ source("PROD_LANDING", "raw_vaults_fyi_data") }}
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
    pool:response:address::string as id,
    pool:response:apy:"1day":total::float as apy,
    p.chain,
    pool:response:protocol:name::string as protocol,
    pool:response:name::string as name,
    pool:response:lendUrl::string as source,
    pool:response:protocolVaultUrl::string as vault_url,
    pool:response:tvl:usd::float as tvl,
    pool:response:tags[0]::string as type,
    p.symbol,
    extraction_date
  from flattened
  inner join {{ ref("vaults_fyi_stablecoin_pool_ids") }} p
  on lower(pool:response:address::string) = lower(p.id)
  and case 
        when lower(pool:response:network:name::string) = 'mainnet' then 'ethereum'
        else lower(pool:response:network:name::string)
      end = lower(p.chain)
)

select
  id
  , name
  , apy
  , tvl
  , array_construct(symbol) as symbol
  , case when lower(protocol) = lower('sky') then 'maker' else protocol end as protocol
  , type
  , chain
  , coalesce(source, vault_url) as link
  , extraction_date as extraction_timestamp
from extracted