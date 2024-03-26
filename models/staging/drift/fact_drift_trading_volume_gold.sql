{{ config(materialized="table") }}
select date, app, category, chain, trading_volume
from {{ ref("fact_drift_trading_volume") }}
