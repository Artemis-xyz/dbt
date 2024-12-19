{{config(materialized='table')}}

select lower(address) as address, 'WOLF_OF_WALL_STREET' as category, array_agg(distinct symbol) as reason
from {{ ref('agg_solana_tokens_held') }}
where 
    (symbol = 'HAWKTUAH' and first_seen <= '2024-11-26')
    or (symbol = 'QUANT' and first_seen < '2024-11-21')
group by 1