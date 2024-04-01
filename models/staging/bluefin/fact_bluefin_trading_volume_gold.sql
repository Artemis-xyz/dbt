{{ config(materialized="table", snowflake_warehouse="BLUEFIN") }}
select date, chain, app, category, trading_volume
from {{ ref("fact_bluefin_trading_volume_silver") }}
where date < to_date(sysdate())
