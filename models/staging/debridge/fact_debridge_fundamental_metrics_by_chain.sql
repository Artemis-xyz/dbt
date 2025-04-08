with
    protocol_volume_and_fees as (
        select date, source_chain as chain, sum(fee_usd) as fees
        from {{ ref("fact_debridge_flows") }}
        group by 1, 2
    ),

    outflows as (
        select date, source_chain as chain, sum(amount_usd) as outflow
        from {{ ref("fact_debridge_flows") }}
        where chain is not null
        group by 1, 2
    ),

    inflows as (
        select date, destination_chain as chain, sum(amount_usd) as inflow
        from {{ ref("fact_debridge_flows") }}
        where chain is not null
        group by 1, 2
    )
    select
        coalesce(t1.date, t2.date) as date,
        coalesce(t1.chain, t2.chain) as chain,
        coalesce(t2.inflow, 0) as inflow,
        coalesce(t1.outflow, 0) as outflow,
        fees,
        'debridge' as app,
        'Bridge' as category
    from protocol_volume_and_fees
    full join outflows t1 on protocol_volume_and_fees.date = t1.date and protocol_volume_and_fees.chain = t1.chain
    full join inflows t2 on protocol_volume_and_fees.date = t2.date and protocol_volume_and_fees.chain = t2.chain
