{{
    config(
        materialized="table",
        snowflake_warehouse="KTX_FINANCE",
        database="ktx_finance",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_ktx_finance_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_ktx_finance_unique_traders") }}
    )
select
    date
    , 'ktx_finance' as app
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
