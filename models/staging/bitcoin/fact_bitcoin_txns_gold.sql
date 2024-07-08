{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
select date, txns, source, chain
from {{ ref("fact_bitcoin_txns") }}
