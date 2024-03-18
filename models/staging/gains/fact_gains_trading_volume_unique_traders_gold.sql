{{ config(materialized="table") }}
select chain, date, trading_volume, unique_traders, app, category
from {{ ref("fact_gains_trading_volume_unique_traders") }}
