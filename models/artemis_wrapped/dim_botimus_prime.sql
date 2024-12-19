{{config(materialized='table')}}

select lower(address) as address, 'BOTIMUS_PRIME' as category, count(distinct date) as reason
from {{ref('agg_base_daily_interactions')}}
where daily_interactions > 1000
group by 1

union all 


select lower(address) as address, 'BOTIMUS_PRIME' as category, count(distinct date) as reason
from {{ref('agg_solana_daily_interactions')}}
where daily_interactions > 1000
group by 1