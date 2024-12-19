{{config(materialized='table')}}

-- BOTEMUS PRIME
select block_timestamp::date as date, from_address as address, count(distinct tx_hash) as daily_interactions
from {{ ref('ez_base_transactions') }}
where block_timestamp > '2023-12-31'
group by 1, 2