{{ config(materialized="table") }}
select date, addresses, chain
from {{ ref("fact_bitcoin_addresses_with_balance_gte_ten") }}
