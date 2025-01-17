{{config(materialized='table')}}


select lower(address) as address, 'SOLANA_TRENCH_WARRIOR' as category, sum(interactions) as reason
from {{ ref('agg_solana_app_interactions') }}
where app in ('pumpdotfun', 'raydium')
group by 1
having reason > 300