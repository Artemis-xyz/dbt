{{ config(materialized="table") }}

select date, trading_volume, app, category, chain
from {{ ref("fact_perpetual_protocol_trading_volume") }}
