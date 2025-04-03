{{
    config(
        materialized="table",
        snowflake_warehouse="AVANTIS",
        database="avantis",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_avantis_trading_volume_silver") }}
        group by date
    )
    , unique_traders_data as (
        select date, sum(unique_traders) as unique_traders
        from {{ ref("fact_avantis_unique_traders_silver") }}
        group by date
    )
select
    date
    , 'avantis' as app
    , 'DeFi' as category
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from trading_volume_data
left join unique_traders_data using(date)
where date < to_date(sysdate())
