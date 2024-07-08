{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data_v1 as (
        select date, trading_volume, unique_traders, chain
        from {{ ref("fact_gmx_trading_volume") }}
        left join {{ ref("fact_gmx_unique_traders") }} using(date, chain)
        where chain is not null
    ),
    v2_data as (
        select date, trading_volume, unique_traders, chain
        from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
        where chain is not null
    ),
    combined_data as (
        select 
            date,
            chain,
            sum(trading_volume) as trading_volume,
            sum(unique_traders) as unique_traders
        from (
            select * from trading_volume_data_v1
            union all
            select * from v2_data
        )
        group by 1, 2
    )

select 
    date as date,
    'gmx' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders
from combined_data
where date < to_date(sysdate())
