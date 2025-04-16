{{
    config(
        materialized="table",
        snowflake_warehouse="AEVO",
        database="aevo",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_aevo_trading_volume") }}
    )
select
    date,
    'aevo' as app,
    'DeFi' as category,
    chain,
    trading_volume
    -- standardize metrics
    , trading_volume as perp_volume
from trading_volume_data
where date < to_date(sysdate())
