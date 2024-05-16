with
    protocol_volume_and_fees as (
        select date, sum(amount_usd) as bridge_volume
        from {{ ref("fact_rainbow_bridge_flows") }}
        group by 1
    ),

    outflows as (
        select date, source_chain as chain, sum(amount_usd) as outflow
        from {{ ref("fact_rainbow_bridge_flows") }}
        where chain is not null
        group by 1, 2
    ),

    inflows as (
        select date, destination_chain as chain, sum(amount_usd) as inflow
        from {{ ref("fact_rainbow_bridge_flows") }}
        where chain is not null
        group by 1, 2
    )

select
    date,
    null as chain,
    null as inflow,
    null as outflow,
    bridge_volume,
    null as fees,
    'rainbow_bridge' as app,
    'Bridge' as category
from protocol_volume_and_fees

union

select
    coalesce(t1.date, t2.date) as date,
    coalesce(t1.chain, t2.chain) as chain,
    coalesce(t2.inflow, 0) as inflow,
    coalesce(t1.outflow, 0) as outflow,
    null as bridge_volume,
    null as fees,
    'rainbow_bridge' as app,
    'Bridge' as category
from outflows t1
full join inflows t2 on t1.date = t2.date and t1.chain = t2.chain
