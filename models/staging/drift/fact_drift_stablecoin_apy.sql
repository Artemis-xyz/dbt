{{ config(materialized="table") }}

with stableconin_lending as (
  select
    l.timestamp
    , l.market as id
    , concat(l.market, ' Main Pool') as name
    , l.apy
    , l.tvl
    , array_construct(p.symbol) as symbol
    , l.protocol
    , l.type
    , l.chain
    , p.link
  from {{ ref("fact_drift_lending_apy") }} l
  join {{ ref("drift_stablecoin_pool_ids") }} p
    on l.market = p.name
),

stablecoin_iv as (
  select
    i.timestamp
    , i.market as id
    , concat(i.market, ' ', p.market) as name
    , i.apy
    , i.tvl
    , array_construct(p.symbol) as symbol
    , i.protocol
    , i.type
    , i.chain
    , p.link
  from {{ ref("fact_drift_insurance_vault_apy") }} i
  join {{ ref("drift_stablecoin_pool_ids") }} p
    on i.market = p.name
)

select
  timestamp
  , id
  , name
  , apy
  , tvl
  , symbol
  , protocol
  , type
  , chain
  , link
from stableconin_lending
union all
select
  timestamp
  , id
  , name
  , apy
  , tvl
  , symbol
  , protocol
  , type
  , chain
  , link
from stablecoin_iv