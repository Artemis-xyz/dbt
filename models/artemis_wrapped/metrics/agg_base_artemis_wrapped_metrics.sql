{{config(materialized='table', snowflake_warehouse='BALANCES_LG')}}
select 
    lower(from_address) as address
    , count(distinct tx_hash) as total_txns
    , sum(gas_usd) as total_gas_paid
    , count(distinct raw_date) as days_onchain
    , count(distinct app) as apps_used
from {{ ref('fact_base_transactions_v2') }}
where block_timestamp > '2023-12-31'
group by 1