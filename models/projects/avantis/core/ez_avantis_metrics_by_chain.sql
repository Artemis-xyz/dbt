{{
    config(
        materialized="table",
        snowflake_warehouse="AVANTIS",
        database="avantis",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_avantis_trading_volume_silver") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_avantis_unique_traders_silver") }}
    )
select
    date
    , 'avantis' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from unique_traders_data
left join trading_volume_data using(date, chain)
where date < to_date(sysdate())
