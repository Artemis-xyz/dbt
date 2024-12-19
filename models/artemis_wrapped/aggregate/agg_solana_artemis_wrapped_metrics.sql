{{config(materialized='table', snowflake_warehouse='BALANCES_LG')}}
with agg_data as (
    select 
        value as address
        , count(distinct tx_hash) as total_txns
        , sum(gas_usd) as total_gas_paid
        , count(distinct raw_date) as days_onchain
        , count(distinct app) as apps_used
    from {{ ref('ez_solana_transactions') }}, 
    lateral flatten(input => signers)
    where block_timestamp > '2023-12-31'
    group by 1
)

select 
    address
    , total_txns
    , NTILE(100000) OVER (ORDER BY total_txns DESC) / 1000.0 AS total_txns_percent_rank
    , total_gas_paid
    , NTILE(100000) OVER (ORDER BY total_gas_paid DESC) / 1000.0 AS total_gas_paid_percent_rank
    , days_onchain
    , NTILE(100000) OVER (ORDER BY days_onchain DESC) / 1000.0 AS days_onchain_percent_rank
    , apps_used
    , NTILE(100000) OVER (ORDER BY apps_used DESC)  / 1000.0 AS apps_used_percent_rank
from agg_data