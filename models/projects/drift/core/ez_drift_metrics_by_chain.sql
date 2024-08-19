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
    trading_data as (
        select date, trading_volume, chain
        from {{ ref("fact_drift_trading_volume") }}
    ),
    prediction_data as (
        select date, trump_price, kamala_price, chain
        from {{ ref("fact_drift_prediction_markets") }}
    )

select
    trading_data.date,
    'drift' as app,
    'DeFi' as category,
    trading_data.chain,
    trading_volume,
    trump_price,
    kamala_price
from trading_data
left join prediction_data
    on trading_data.date = prediction_data.date
where trading_data.date < to_date(sysdate())