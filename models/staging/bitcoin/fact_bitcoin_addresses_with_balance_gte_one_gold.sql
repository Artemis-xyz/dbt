{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
select date, addresses, chain
from {{ ref("fact_bitcoin_addresses_with_balance_gte_one") }}
