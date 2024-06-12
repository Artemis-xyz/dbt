{{
    config(
        materialized="table",
        snowflake_warehouse="BLUEFIN",
        database="bluefin",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_bluefin_trading_volume_silver") }}
    )
select
    date,
    'bluefin' as app,
    'DeFi' as category,
    chain,
    trading_volume
from trading_volume_data
where date < to_date(sysdate())