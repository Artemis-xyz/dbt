{% macro fact_daily_uniswap_v3_fork_trading_vol_and_fees(
    token_address, chain, blacklist, app
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
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pairs_not_blacklisted t2 on lower(t1.contract_address) = lower(t2.pool)
            where
                t1.event_name in ('Swap')
                {% if is_incremental() %}
                    and block_timestamp
                    >= (select max(date) + interval '1 DAY' from {{ this }})
                {% endif %}
                and trunc(t1.block_timestamp, 'day')
                < trunc(convert_timezone('UTC', sysdate()), 'day')
        ),
        swaps as (
            select
                trunc(block_timestamp, 'hour') as hour,
                tx_hash,
                pool,
                token0,
                decoded_log:"amount0"::float as token0_amount,
                token1,
                decoded_log:"amount1"::float as token1_amount,
                fee
            from all_pool_events
        ),
        swaps_asdjusted as (
            select
                t1.hour,
                tx_hash,
                pool,
                token0,
                t2.decimals as token0_decimals,
                token0_amount / pow(10, token0_decimals) as token0_amount_adj,
                t2.price as token0_price,
                ifnull(token0_price * abs(token0_amount_adj), 0) as token0_amount_usd,
                token1,
                t3.decimals as token1_decimals,
                token1_amount / pow(10, token1_decimals) as token1_amount_adj,
                t3.price as token1_price,
                ifnull(token1_price * abs(token1_amount_adj), 0) as token1_amount_usd,
                case
                    when token0_amount_adj > 0
                    then token0_amount_usd * fee
                    else token1_amount_usd * fee
                end as token_fee_amount,
                fee
            from swaps t1

            left join
                {{ chain }}_flipside.price.ez_hourly_token_prices t2
                on (lower(t1.token0) = lower(t2.token_address) and t2.hour = t1.hour)
            left join
                {{ chain }}_flipside.price.ez_hourly_token_prices t3
                on (lower(t1.token1) = lower(t3.token_address) and t3.hour = t1.hour)
            where token1_decimals != 0 and token0_decimals != 0
        ),
        filtered_pairs as (
            select
                trunc(hour, 'day') as date,
                least(token0_amount_usd, token1_amount_usd) as volume_per_trade,
                case
                    when volume_per_trade > token_fee_amount
                    then token_fee_amount
                    else 0
                end as token_fee_amount
            from swaps_asdjusted
        )
    select
        date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        'DeFi' as category,
        sum(volume_per_trade) as trading_volume,
        sum(token_fee_amount) as fees
    from filtered_pairs
    where date is not null
    group by date
{% endmacro %}
