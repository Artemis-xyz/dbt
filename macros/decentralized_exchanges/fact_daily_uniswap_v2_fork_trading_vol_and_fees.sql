{% macro fact_daily_uniswap_v2_fork_trading_vol_and_fees(
    token_address, chain, blacklist, app, fees
) %}
    with
        pools as (
            select
                decoded_log:"pair"::string as pair,
                decoded_log:"token0"::string as token0,
                decoded_log:"token1"::string as token1,
                {{ fees }}/ 1E6 as fee
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ token_address }}')
                and event_name = 'PairCreated'
        ),
        pairs_not_blacklisted as (
            select *
            from pools
            {% if blacklist is string %} where lower(pair) != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} where pair not in {{ blacklist }}
            {% endif %}
        ),
        all_pool_events as (
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pairs_not_blacklisted t2 on lower(t1.contract_address) = lower(t2.pair)
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
                pair,
                token0,
                decoded_log:"amount0In"::float as token0_in_amount,
                decoded_log:"amount0Out"::float as token0_out_amount,
                token1,
                decoded_log:"amount1In"::float as token1_in_amount,
                decoded_log:"amount1Out"::float as token1_out_amount,
                token0_in_amount * fee as token0_in_fee,
                token0_out_amount * fee as token0_out_fee,
                token1_in_amount * fee as token1_in_fee,
                token1_out_amount * fee as token1_out_fee
            from all_pool_events
        ),
        swaps_asdjusted as (
            select
                t1.hour,
                tx_hash,
                pair,

                token0,
                t2.decimals as token0_decimals,
                coalesce(t2.price, 0) as token0_price,
                token0_in_fee
                / pow(10, token0_decimals)
                * token0_price as token0_in_fee_usd,
                token0_out_fee
                / pow(10, token0_decimals)
                * token0_price as token0_out_fee_usd,
                token0_in_amount
                / pow(10, token0_decimals)
                * token0_price as token0_in_amount_usd,
                token0_out_amount
                / pow(10, token0_decimals)
                * token0_price as token0_out_amount_usd,

                token1,
                t3.decimals as token1_decimals,
                coalesce(t3.price, 0) as token1_price,
                token1_in_fee
                / pow(10, token1_decimals)
                * token1_price as token1_in_fee_usd,
                token1_out_fee
                / pow(10, token1_decimals)
                * token1_price as token1_out_fee_usd,
                token1_in_amount
                / pow(10, token1_decimals)
                * token1_price as token1_in_amount_usd,
                token1_out_amount
                / pow(10, token1_decimals)
                * token1_price as token1_out_amount_usd,

                case
                    when
                        (token0_in_fee_usd + token1_in_fee_usd) * 10
                        > (token0_out_fee_usd + token1_out_fee_usd)
                    then token0_out_fee_usd + token1_out_fee_usd
                    else token0_in_fee_usd + token1_in_fee_usd
                end as total_fees,
                token0_in_amount_usd + token1_in_amount_usd as total_in,
                token0_out_amount_usd + token1_out_amount_usd as total_out
            from swaps t1
            left join
                {{ chain }}_flipside.price.ez_hourly_token_prices t2
                on (lower(t1.token0) = lower(t2.token_address) and t2.hour = t1.hour)
            left join
                {{ chain }}_flipside.price.ez_hourly_token_prices t3
                on (lower(t1.token1) = lower(t3.token_address) and t3.hour = t1.hour)
            where token1_decimals > 0 and token0_decimals > 0
        ),
        filtered_pairs as (
            select
                trunc(hour, 'day') as date,
                least(total_out, total_in) as volume_per_trade,
                total_fees
            from swaps_asdjusted
        )
    select
        date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        'DeFi' as category,
        sum(volume_per_trade) as trading_volume,
        sum(total_fees) as fees
    from filtered_pairs
    where date is not null
    group by date
{% endmacro %}
