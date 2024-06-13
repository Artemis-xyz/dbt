{{ config(materialized="table", snowflake_warehouse="COSMOSHUB") }}
select date, txns, chain
from {{ ref("fact_cosmoshub_txns") }}
