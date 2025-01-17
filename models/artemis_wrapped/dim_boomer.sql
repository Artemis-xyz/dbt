{{config(materialized='table')}}

select lower(address) as address, 'BOOMER' as category, symbols as reason
from {{ ref('agg_base_stablecoin_classification')}}

union all 

select lower(address) as address, 'BOOMER' as category, symbols as reason
from {{ ref('agg_solana_stablecoin_classification')}}