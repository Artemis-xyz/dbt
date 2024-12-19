{{config(materialized='table')}}

select lower(address) as address, 'DORA_THE_EXPLORER' as category, count(distinct app) as reason
from {{ ref('agg_base_app_interactions')}}
group by 1
having reason >= 3 and reason <= 20
union all
select lower(address) as address, 'DORA_THE_EXPLORER' as category, count(distinct app) as reason
from {{ ref('agg_solana_app_interactions')}}
group by 1
having reason >= 3 and reason <= 20
