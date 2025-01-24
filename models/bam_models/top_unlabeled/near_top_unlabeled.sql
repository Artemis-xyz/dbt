{{ config(materialized="table") }}
with
    contracts as (
        select address, name from {{ ref("dim_all_addresses_labeled_gold") }} where chain = 'near'
    )
select t.tx_receiver address
from near_flipside.core.fact_transactions as t
left join contracts on lower(t.tx_receiver) = lower(contracts.address)
where
    t.block_timestamp > dateadd(day, -7, current_date())
    and contracts.name is null
    and tx_receiver is not null
group by t.tx_receiver
order by sum(transaction_fee) / pow(10, 24) desc
limit 500
