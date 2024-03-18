{{ config(materialized="table") }}

select date, trading_volume, app, category, chain
from {{ ref("fact_hyperliquid_trading_volume") }}
