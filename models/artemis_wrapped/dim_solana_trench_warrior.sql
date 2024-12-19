{{config(materialized='table')}}


select lower(address) as address, 'SOLANA_TRENCH_WARRIOR' as category, array_agg(app) as reason
from {{ ref('agg_solana_app_interactions') }}
where interactions > 300 and app in ('pumpdotfun', 'raydium')
group by 1