{{config(materialized='table', snowflake_warehouse='BALANCES_LG')}}

select block_timestamp::date as date, value as address, count(distinct tx_hash) as daily_interactions
from {{ ref('ez_solana_transactions') }}, 
    lateral flatten(input => signers)
where block_timestamp > '2023-12-31'
group by 1, 2