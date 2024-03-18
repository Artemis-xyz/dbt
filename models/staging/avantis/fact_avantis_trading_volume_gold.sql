{{ config(materialized="table", snowflake_warehouse="AVANTIS") }}
select date, chain, app, category, trading_volume
from {{ ref("fact_avantis_trading_volume_silver") }}
where date < to_date(sysdate())
