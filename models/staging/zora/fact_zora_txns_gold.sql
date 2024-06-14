{{ config(materialized="table", snowflake_warehouse="ZORA") }}
select date, txns, chain
from {{ ref("fact_zora_txns") }}
