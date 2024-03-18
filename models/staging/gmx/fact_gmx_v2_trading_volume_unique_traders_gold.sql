{{ config(materialized="table") }}

select date, trading_volume, unique_traders, app, category, chain
from {{ ref("fact_gmx_v2_trading_volume_unique_traders") }}
