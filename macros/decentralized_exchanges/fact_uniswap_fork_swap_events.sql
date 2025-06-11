{% macro fact_uniswap_v2_fork_swap_events(
    factory, chain, app='uniswap', version='v2'
) %}
    with
        pools as (
            select
                decoded_log:"pair"::string as pair,
                decoded_log:"token0"::string as token0,
                decoded_log:"token1"::string as token1
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ factory }}')
                and event_name = 'PairCreated'
        ),
        all_pool_events as (
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp, t1.event_index
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pools t2 on lower(t1.contract_address) = lower(t2.pair)
            where
                t1.event_name in ('Swap')
                {% if is_incremental() %}
                    and block_timestamp >= (select DATEADD('day', -3, max(block_timestamp)) from {{ this }})
                {% endif %}
        )
    select
        block_timestamp,
        event_index,
        tx_hash,
        '{{chain}}' as chain,
        decoded_log:"sender"::string as sender,
        decoded_log:"to"::string as recipient,
        pair,
        token0,
        t0.symbol as token0_symbol,
        abs(decoded_log:"amount0In"::float - decoded_log:"amount0Out"::float) as raw_token0_amount,
        abs(decoded_log:"amount0In"::float - decoded_log:"amount0Out"::float) / pow(10, t0.decimals) as token0_amount,
        token1,
        t1.symbol as token1_symbol, 
        abs(decoded_log:"amount1In"::float - decoded_log:"amount1Out"::float) as raw_token1_amount,
        abs(decoded_log:"amount1In"::float - decoded_log:"amount1Out"::float) / pow(10, t1.decimals) as token1_amount
    from all_pool_events
    left join {{chain}}_flipside.core.dim_contracts t0 on lower(token0) = lower(t0.address) 
    left join {{chain}}_flipside.core.dim_contracts t1 on lower(token1) = lower(t1.address) 
{% endmacro %}


{% macro fact_uniswap_v3_fork_swap_events(factory, chain, app, version) %}
    with
        pools as (
            select
                decoded_log:"pool"::string as pool,
                decoded_log:"token0"::string as token0,
                decoded_log:"token1"::string as token1,
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ factory }}')
                and event_name = 'PoolCreated'
        )
        , all_pool_events as (
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp, event_index
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pools t2 on lower(t1.contract_address) = lower(t2.pool)
            where
                t1.event_name in ('Swap')
                {% if is_incremental() %}
                    and block_timestamp
                    >= (select max(block_timestamp) from {{ this }})
                {% endif %}
        )
    select
        block_timestamp,
        event_index,
        tx_hash,
        '{{chain}}' as chain,
        decoded_log:"sender"::string as sender,
        decoded_log:"recipient"::string as recipient,
        pool,
        token0,
        t0.symbol as token0_symbol,
        abs(decoded_log:"amount0"::float) as raw_token0_amount,
        raw_token0_amount / pow(10, t0.decimals) as token0_amount,
        token1,
        t1.symbol as token1_symbol, 
        abs(decoded_log:"amount1"::float) as raw_token1_amount,
        raw_token1_amount / pow(10, t1.decimals) as token1_amount
    from all_pool_events
    left join {{chain}}_flipside.core.dim_contracts t0 on lower(token0) = lower(t0.address) 
    left join {{chain}}_flipside.core.dim_contracts t1 on lower(token1) = lower(t1.address) 
{% endmacro %}