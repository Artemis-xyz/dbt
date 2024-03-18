{{ config(materialized="table") }}

select date, trading_volume, app, category, chain
from {{ ref("fact_rabbitx_trading_volume") }}
