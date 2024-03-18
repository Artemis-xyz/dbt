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

    trading_volume_data as (
        select
            date_trunc('day', logs.block_timestamp) as date,
            sum(
                abs(
                    -- hex_to_int_with_encoding is a function that exists in our
                    -- snowflake db
                    -- The exact code can be found in hex_to_int_udf.sql file in the
                    -- scripts folder
                    {{ target.schema }}.hex_to_int_with_encoding(
                        's2c', substring(logs.data, 451, 64)
                    )::bigint
                )
                / 1e18
            ) as trading_volume
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
            -- logs.decoded_log:"isTaker" = TRUE
            and substring(logs.data, 322, 1) = '1'
            and exists (
                select 1
                from vertex_perp_contract_addresses
                where
                    lower(logs.contract_address)
                    = lower(vertex_perp_contract_addresses.contract_address)
            )
        group by 1
        order by 1 desc
    ),

    results as (
        select
            'arbitrum' as chain,
            date,
            trading_volume,
            'vertex' as app,
            'DeFi' as category
        from trading_volume_data
    )

select chain, app, category, date, trading_volume
from results
