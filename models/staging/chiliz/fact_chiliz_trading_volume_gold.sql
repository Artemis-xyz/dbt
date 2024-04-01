{{ config(materialized="table") }}
select date, market_pair, trading_volume, chain
from {{ ref("fact_chiliz_trading_volume") }}
