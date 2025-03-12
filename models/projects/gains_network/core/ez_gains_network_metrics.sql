{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK",
        database="gains_network",
        schema="core",
        alias="ez_metrics"
    )
}}

with date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between '2022-12-14' and to_date(sysdate())
)

    , gains_data as (
        select date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders
        from {{ ref("fact_gains_trading_volume_unique_traders") }}
        group by date
    )
    , gains_fees as (
        select date, fees, revenue
        from {{ ref("fact_gains_fees") }}
    )
    , gains_tvl as (
        select date, sum(usd_balance) as tvl
        from {{ ref("fact_gains_tvl") }}
        group by date
    )

select
    ds.date,
    'gains-network' as app,
    'DeFi' as category,
    gd.trading_volume,
    gd.unique_traders,
    gf.fees,
    gf.revenue,
    gt.tvl
from date_spine ds
left join gains_data gd using (date)
left join gains_fees gf using (date)
left join gains_tvl gt using (date)
