{{config(materialized='table')}}

select lower(address) as address, 'OLD_MCDONALD' as category, array_agg(distinct app) as reason
from {{ ref('agg_base_app_interactions') }}
where app in ('aave', 'uniswap', 'aerodrome', 'sushiswap') and interactions > 5
group by 1

union all 

select lower(address) as address, 'OLD_MCDONALD' as category, array_agg(distinct app) as reason
from {{ ref('agg_solana_app_interactions') }}
where app in ('jupiter', 'drift', 'magic_eden', 'kamino', 'selenium') and interactions > 5
group by 1