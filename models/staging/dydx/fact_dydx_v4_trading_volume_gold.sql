{{ config(materialized="table") }}
select market_pair, date, trading_volume, app, category, chain
from {{ ref("fact_dydx_v4_trading_volume") }}
