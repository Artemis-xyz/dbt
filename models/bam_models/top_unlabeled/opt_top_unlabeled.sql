{{ config(materialized="table") }}
with
    contracts as (
        select address, name
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'optimism'
    )
select t.to_address address
from optimism_flipside.core.fact_transactions as t
left join contracts on lower(t.to_address) = lower(contracts.address)
where
    t.block_timestamp > dateadd(day, -7, current_date())
    and contracts.name is null
    and to_address is not null
group by t.to_address
order by sum(gas_price * gas_used) / 1e9 desc
limit 500
