{{
    config(
        materialized="table",
        snowflake_warehouse="SYNTHETIX",
        database="synthetix",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_synthetix_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_synthetix_unique_traders") }}
    ), 
    tvl as (
        select 
            date,
            chain,
            sum(tvl_usd) as tvl_usd,
        from {{ ref("fact_synthetix_tvl_by_chain_and_token") }}
        group by 1,2 
    )
select
    date,
    'synthetix' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders,
    tvl_usd
from unique_traders_data
left join trading_volume_data using(date, chain)
left join tvl using(date, chain)
where date < to_date(sysdate())