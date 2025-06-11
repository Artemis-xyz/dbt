{{
    config(
        materialized="table",
        snowflake_warehouse="HOLDSTATION",
        database="holdstation",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_holdstation_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_holdstation_unique_traders") }}
    )
select
    date
    , 'holdstation' as app
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