{{ config(materialized="table") }}

select
    d.date as timestamp,
    d.market as id,
    concat(d.market, ' ', p.market) as name,
    d.daily_avg_deposit_rate / 100 as apy,
    (d.daily_avg_user_balance / (1 - (d.daily_avg_utilization / 100))) as tvl,
    array_construct(p.symbol) as symbol,
    'drift' as protocol,
    'lending' as type,
    p.link
from {{ ref("fact_drift_daily_spot_data") }} d
inner join {{ ref("drift_stablecoin_pool_ids") }} p
    on d.market = p.name
where d.date = current_date
