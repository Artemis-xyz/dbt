{{ config(materialized="table") }}

select date, trading_volume, app, category, chain
from {{ ref("fact_level_finance_trading_volume") }}
