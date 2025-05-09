{{ config(materialized="table") }}

with latest_date as (
  select max(date) as max_date from {{ ref("fact_drift_daily_spot_data") }}
)

select
    d.date as timestamp,
    d.market as id,
    concat(d.market, ' ', p.market) as name,
    d.daily_avg_deposit_rate / 100 as apy,
    d.daily_avg_user_balance + d.daily_avg_protocol_balance as tvl,
    array_construct(p.symbol) as symbol,
    'drift' as protocol,
    'Lending' as type,
    'solana' as chain,
    p.link
from {{ ref("fact_drift_daily_spot_data") }} d
join latest_date ld on d.date = ld.max_date
inner join {{ ref("drift_stablecoin_pool_ids") }} p
    on d.market = p.name
