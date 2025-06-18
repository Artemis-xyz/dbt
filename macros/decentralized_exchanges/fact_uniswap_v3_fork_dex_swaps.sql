{% macro fact_uniswap_v3_fork_dex_swaps(
    token_address, chain, blacklist, app, version='v3'
) %}
    with
        pools as (
            select
                decoded_log:"pool"::string as pool,
                decoded_log:"token0"::string as token0,
                decoded_log:"token1"::string as token1,
                decoded_log:"fee"::float / 1e6 as fee
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ token_address }}')
                and event_name = 'PoolCreated'
        ),
        pairs_not_blacklisted as (
            select *
            from pools
            {% if blacklist is string %} where lower(pool) != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} where lower(pool) not in {{ blacklist }}
            {% endif %}
        ),
        all_pool_events as (
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp, event_index
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pairs_not_blacklisted t2 on lower(t1.contract_address) = lower(t2.pool)
            where
                t1.event_name in ('Swap')
                {% if is_incremental() %}
                    and block_timestamp
                    >= (select max(block_timestamp) from {{ this }})
                {% endif %}
        ),
        swaps as (
            select
                block_timestamp,
                trunc(block_timestamp, 'hour') as hour,
                tx_hash,
                event_index,
                pool,
                token0,
                decoded_log:"amount0"::float as token0_amount,
                token1,
                decoded_log:"amount1"::float as token1_amount,
                decoded_log:"sender"::string as sender,
                decoded_log:"recipient"::string as recipient,
                fee
            from all_pool_events
        ),
        swaps_adjusted as (
            select
                t1.block_timestamp,
                t1.hour,
                tx_hash,
                event_index,
                sender,
                recipient,
                pool,
                token0,
                t2.symbol as token0_symbol,
                t2.decimals as token0_decimals,
                token0_amount / pow(10, token0_decimals) as token0_amount_adj,
                t2.price as token0_price,
                ifnull(token0_price * abs(token0_amount_adj), 0) as token0_amount_usd,
                token1,
                t3.symbol as token1_symbol,
                t3.decimals as token1_decimals,
                token1_amount / pow(10, token1_decimals) as token1_amount_adj,
                t3.price as token1_price,
                ifnull(token1_price * abs(token1_amount_adj), 0) as token1_amount_usd,
                case
                    when token0_amount_adj > 0
                    then token0_amount_usd * fee
                    else token1_amount_usd * fee
                end as token_fee_amount,
                case when token0_amount_adj > 0
                    then token0_amount_adj * fee
                    else token1_amount_adj * fee
                end as token_fee_amount_native,
                case when token0_amount_adj > 0
                    then token0_symbol
                    else token1_symbol
                end as token_fee_amount_native_symbol,
                fee
            from swaps t1

            left join
                {{ chain }}_flipside.price.ez_prices_hourly t2
                on (lower(t1.token0) = lower(t2.token_address) and t2.hour = t1.hour)
            left join
                {{ chain }}_flipside.price.ez_prices_hourly t3
                on (lower(t1.token1) = lower(t3.token_address) and t3.hour = t1.hour)
            where token1_decimals != 0 and token0_decimals != 0
        ),
        filtered_pairs as (
            select
                block_timestamp,
                tx_hash,
                event_index,
                sender,
                recipient,
                pool,
                token0,
                token0_symbol,
                token0_amount_adj as token_0_volume_native,
                token1,
                token1_symbol,
                token1_amount_adj as token_1_volume_native,
                least(token0_amount_usd, token1_amount_usd) as volume_per_trade,
                case
                    when volume_per_trade > token_fee_amount
                    then token_fee_amount
                    else 0
                end as token_fee_amount,
                case
                    when volume_per_trade > token_fee_amount
                    then token_fee_amount_native
                    else 0
                end as token_fee_amount_native,
                token_fee_amount_native_symbol, 
                fee as fee_percent
            from swaps_adjusted
        ),
        events as (
            select
                block_timestamp,
                tx_hash,
                event_index,
                sender,
                recipient,
                pool,
                token0 as token_0,
                token0_symbol as token_0_symbol,
                token_0_volume_native,
                token1 as token_1,
                token1_symbol as token_1_symbol,
                token_1_volume_native,
                volume_per_trade as trading_volume,
                token_fee_amount as trading_fees,
                token_fee_amount_native,
                token_fee_amount_native_symbol,
                fee_percent,
                ROW_NUMBER() OVER (PARTITION by tx_hash, pool ORDER BY event_index) AS row_number
            from filtered_pairs
        ),
        traces as (
            select
                t1.*,
                {% if chain in ("arbitrum") %}
                    t3.gas_price_paid as gas_price,
                {% else %}
                    t3.gas_price,
                {% endif %}
                ROW_NUMBER() OVER (PARTITION by t1.tx_hash, t1.to_address ORDER BY t1.trace_index) AS row_number
            from  {{ chain }}_flipside.core.fact_traces t1
            inner join filtered_pairs t2 on
                t1.tx_hash = t2.tx_hash
                and lower(t1.to_address) = lower(t2.pool)
                and substr(t1.input, 0, 10) = '0x128acb08' --Swap function
            left join {{ chain }}_flipside.core.fact_transactions t3 on t1.tx_hash = t3.tx_hash
            {% if is_incremental() %}
                where t1.block_timestamp
                >= (select max(block_timestamp) from {{ this }})
            {% endif %}
        )
    select
        events.block_timestamp,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        '{{ version }}' as version,
        'DeFi' as category,
        events.tx_hash,
        event_index,
        sender,
        recipient,
        pool,
        token_0,
        token_0_symbol,
        token_1,
        token_1_symbol,
        trading_volume,
        trading_fees,
        {% if app == 'uniswap' %}
            token_0_volume_native,
            token_1_volume_native,
            token_fee_amount_native,
            token_fee_amount_native_symbol,
        {% endif %}
        {% if app == 'pancakeswap' %}
            fee_percent,
        {% endif %}
        gas_price * gas_used as raw_gas_cost_native,
        raw_gas_cost_native / 1e9 as gas_cost_native, 
    from events
    left join traces on
        events.tx_hash = traces.tx_hash
        and events.pool = traces.to_address
        and events.row_number = traces.row_number
    where events.block_timestamp is not null
{% endmacro %}
