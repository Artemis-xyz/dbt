{{
    config(
        materialized="table",
        snowflake_warehouse="GMX",
        database="gmx",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data_v1 as (
        select date, trading_volume, unique_traders
        from {{ ref("fact_gmx_trading_volume") }}
        left join {{ ref("fact_gmx_unique_traders") }} using(date)
        where chain is not null
    ),
    v2_data as (
        select date, trading_volume, unique_traders
        from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
        where chain is not null
    ),
    combined_data as (
        select 
            date,
            sum(trading_volume) as trading_volume,
            sum(unique_traders) as unique_traders
        from (
            select * from trading_volume_data_v1
            union all
            select * from v2_data
        )
        group by 1
    )
    , fees_data as (
        select date, fees, revenue, supply_side_revenue
        from {{ ref("fact_gmx_all_versions_fees") }}
    )

select 
    date as date,
    'gmx' as app,
    'DeFi' as category,
    trading_volume,
    unique_traders,
    fees,
    revenue,
    supply_side_revenue
from combined_data
left join fees_data using(date)
where date < to_date(sysdate())