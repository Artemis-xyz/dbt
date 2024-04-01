{{ config(materialized="table") }}

select date, trading_volume, unique_traders, app, category, chain
from {{ ref("fact_mux_trading_volume_unique_traders") }}
where date < to_date(sysdate())
