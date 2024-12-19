{{config(materialized='table', snowflake_warehouse='BALANCES_LG')}}

select value as address, app, count(distinct tx_hash) as interactions
from {{ref('ez_solana_transactions')}}, 
    lateral flatten(input => signers)
where block_timestamp > '2023-12-31' and app is not null
group by 1, 2