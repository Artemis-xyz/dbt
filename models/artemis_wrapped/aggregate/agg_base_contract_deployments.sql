{{config(materialized='table')}}

-- BOB THE BUILDER
select from_address as address, count(distinct to_address) as contract_deployments
from base_flipside.core.fact_traces
where type in ('CREATE', 'CREATE2')
    and block_timestamp > '2023-12-31'
group by 1