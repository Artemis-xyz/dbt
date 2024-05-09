with
    protocol_volume_and_fees as (
        select date, sum(coalesce(amount_usd, 0)) as bridge_volume, sum(coalesce(fee_usd, 0)) as fees
        from {{ ref("fact_starknet_bridge_flows") }}
        group by 1
    ),

    outflows as (
        select date, source_chain as chain, sum(coalesce(amount_usd, 0)) as outflow
        from {{ ref("fact_starknet_bridge_flows") }}
        where chain is not null
        group by 1, 2
    ),

    inflows as (
        select date, destination_chain as chain, sum(coalesce(amount_usd, 0)) as inflow
        from {{ ref("fact_starknet_bridge_flows") }}
        where chain is not null
        group by 1, 2
    )

select
    date,
    null as chain,
    null as inflow,
    null as outflow,
    bridge_volume,
    fees,
    'starknet_bridge' as app,
    'Bridge' as category
from protocol_volume_and_fees
where date >= '2020-05-01'

union

select
    coalesce(t1.date, t2.date) as date,
    coalesce(t1.chain, t2.chain) as chain,
    t2.inflow as inflow,
    t1.outflow as outflow,
    null as bridge_volume,
    null as fees,
    'starknet_bridge' as app,
    'Bridge' as category
from outflows t1
full join inflows t2 on t1.date = t2.date and t1.chain = t2.chain
where coalesce(t1.date, t2.date) >= '2020-05-01'