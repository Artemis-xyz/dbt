{{config(materialized='table')}}
-- BLOCKBUSTER
-- DORA THE EXPLORER
-- OLD MACDONALD
-- SOLANA TRENCH WARRIOR
select from_address as address, app, count(distinct tx_hash) as interactions
from {{ref('fact_base_transactions_v2')}} 
where block_timestamp > '2023-12-31' and app is not null
group by 1, 2
