{{
    config(
        materialized="table",
        snowflake_warehouse="APEX",
        database="apex",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_apex_trading_volume") }}
    )
select
    date
    , 'apex' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    -- standardize metrics
    , trading_volume as perp_volume
from trading_volume_data
where date < to_date(sysdate())
