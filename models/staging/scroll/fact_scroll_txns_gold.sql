{{ config(materialized="table", snowflake_warehouse="SCROLL") }}
select date, txns, chain
from {{ ref("fact_scroll_txns") }}
