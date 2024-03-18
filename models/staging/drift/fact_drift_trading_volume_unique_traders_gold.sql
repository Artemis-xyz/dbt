{{ config(materialized="table") }}
select app, category, chain, market_pair, date, trading_volume, unique_traders
from {{ ref("fact_drift_trading_volume_unique_traders") }}
