{{ config(materialized="table") }}

with latest_stableconin_lending as (
  select
    market
    , extraction_timestamp
  from {{ ref("fact_drift_lending_apy") }}
  qualify row_number() over (partition by market order by extraction_timestamp desc) = 1
),

latest_stablecoin_iv as (
  select
    market
    , extraction_timestamp
  from {{ ref("fact_drift_insurance_vault_apy") }}
  qualify row_number() over (partition by market order by extraction_timestamp desc) = 1
)

select
  l.extraction_timestamp as timestamp
  , l.market as id
  , concat(l.market, ' Main Pool') as name
  , l.apy
  , l.tvl
  , l.symbol
  , l.protocol
  , l.type
  , l.chain
  , l.link
  , a.tvl_score
from {{ ref("fact_drift_lending_apy") }} l
join latest_stableconin_lending ll
  on l.market = ll.market
  and l.extraction_timestamp = ll.extraction_timestamp
join {{ ref("agg_drift_stablecoin_apy") }} a
  on l.market = a.market
  and l.type = a.type
union all
select
  i.extraction_timestamp as timestamp
  , i.market as id
  , concat(i.market, ' Insurance Vault') as name
  , i.apy
  , i.tvl
  , i.symbol
  , i.protocol
  , i.type
  , i.chain
  , i.link
  , a.tvl_score
from {{ ref("fact_drift_insurance_vault_apy") }} i
join latest_stablecoin_iv li
  on i.market = li.market
  and i.extraction_timestamp = li.extraction_timestamp
join {{ ref("agg_drift_stablecoin_apy") }} a
  on i.market = a.market
  and i.type = a.type