{{ config(materialized="table") }}

select date, trading_volume, app, category, chain
from {{ ref("fact_aevo_trading_volume") }}
