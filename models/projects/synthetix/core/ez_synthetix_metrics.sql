{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_synthetix_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_synthetix_unique_traders") }}
    ), 
    tvl as (
        select 
            date,
            sum(tvl_usd) as tvl_usd,
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
        group by 1 
    ),
    net_deposits as (
        select
            date,
            sum(net_deposits) as net_deposits
        from {{ ref("fact_synthetix_net_deposits_by_chain") }}
        group by 1
    )
select
    date,
    'synthetix' as app,
    'DeFi' as category,
    trading_volume,
    unique_traders,
    tvl_usd,
    net_deposits
from unique_traders_data
left join trading_volume_data using(date)
left join tvl using(date)
left join net_deposits using(date)
where date < to_date(sysdate())