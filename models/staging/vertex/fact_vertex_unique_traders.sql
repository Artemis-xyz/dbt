{{ config(materialized="table") }}
with
    vertex_creation_txns as (
        select txns.tx_hash
        from arbitrum_flipside.core.fact_transactions as txns
        where
            lower(txns.to_address) = lower('0xb74C78cca0FADAFBeE52B2f48A67eE8c834b5fd1')
    ),

    vertex_perp_contract_addresses as (
        select logs.contract_address
        from vertex_creation_txns
        inner join
            arbitrum_flipside.core.fact_event_logs as logs
            on vertex_creation_txns.tx_hash = logs.tx_hash
        where
            logs.contract_address != lower('0xb74C78cca0FADAFBeE52B2f48A67eE8c834b5fd1')
        group by logs.contract_address
        order by logs.contract_address
    ),

    unique_traders_data as (
        select
            date_trunc('day', logs.block_timestamp) as date,
            -- logs.topics[2] = subaccount (vertex treats each subaccount as a
            -- distinct user)
            count(distinct lower(logs.topics[2])) as unique_traders
        from arbitrum_flipside.core.fact_event_logs as logs
        inner join
            arbitrum_flipside.core.fact_transactions as txns
            on logs.tx_hash = txns.tx_hash
        where
            txns.to_address = lower('0xbbee07b3e8121227afcfe1e2b82772246226128e')
            -- logs.EVENT_NAME = 'FillOrder'
            and logs.topics[0] = lower(
                '0x224253ad5cda2459ff587f559a41374ab9243acbd2daff8c13f05473db79d14c'
            )
        group by 1
        order by 1 desc
    ),

    results as (
        select
            'arbitrum' as chain,
            date,
            unique_traders,
            'vertex' as app,
            'DeFi' as category
        from unique_traders_data
    )

select chain, app, category, date, unique_traders
from results
