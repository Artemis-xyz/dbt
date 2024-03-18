{{ config(materialized="table") }}
select chain, date, app, trading_volume, category
from {{ ref("fact_gmx_trading_volume") }}
