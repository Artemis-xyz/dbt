{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    drfit_data as (
        select date, trading_volume, chain
        from {{ ref("fact_drift_trading_volume") }}
    )
select
    date,
    'drift' as app,
    'DeFi' as category,
    chain,
    trading_volume
from drfit_data
where date < to_date(sysdate())