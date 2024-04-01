{{ config(materialized="table", snowflake_warehouse="AVANTIS") }}
select date, chain, app, category, unique_traders
from {{ ref("fact_avantis_unique_traders_silver") }}
where date < to_date(sysdate())
