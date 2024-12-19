{{config(materialized='table')}}


select lower(address) as address, 'TERMINALLY_ONBASE' as category, count(distinct date) as reason
from {{ ref('agg_base_daily_interactions')}}
group by 1
having reason > 100