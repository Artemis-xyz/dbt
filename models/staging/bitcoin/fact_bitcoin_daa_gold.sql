{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
select date, daa, source, chain
from {{ ref("fact_bitcoin_daa") }}
