{% macro fact_uniswap_v2_fork_dex_swaps(
    token_address, chain, blacklist, app, fees, version='v2'
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
            select t2.*, t1.tx_hash, t1.block_number, t1.decoded_log, t1.block_timestamp, t1.event_index
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                pairs_not_blacklisted t2 on lower(t1.contract_address) = lower(t2.pair)
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
                event_index,
                trunc(block_timestamp, 'hour') as hour,
                tx_hash,
                decoded_log:"sender"::string as sender,
                decoded_log:"to"::string as recipient,
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
                token1_out_amount * fee as token1_out_fee, 
                fee as fee_percent
            from all_pool_events
        ),
        swaps_adjusted as (
            select
                t1.block_timestamp,
                t1.hour,
                tx_hash,
                sender,
                recipient,
                event_index,
                pair,

                token0,
                t2.symbol as token0_symbol,
                t2.decimals as token0_decimals,
                coalesce(t2.price, 0) as token0_price,
                token0_in_fee * token0_price / pow(10, token0_decimals) as token0_in_fee_usd,
                token0_out_fee * token0_price / pow(10, token0_decimals) as token0_out_fee_usd,
                token0_in_amount * token0_price / pow(10, token0_decimals)  as token0_in_amount_usd,
                token0_out_amount * token0_price / pow(10, token0_decimals) as token0_out_amount_usd,
                (token0_in_amount + token0_out_amount) / pow(10, token0_decimals) as token0_amount_native,
                (token0_in_fee + token0_out_fee) / pow(10, token0_decimals) as token0_fee_amount_native,

                token1,
                t3.symbol as token1_symbol,
                t3.decimals as token1_decimals,
                coalesce(t3.price, 0) as token1_price,
                token1_in_fee * token1_price / pow(10, token1_decimals) as token1_in_fee_usd,
                token1_out_fee * token1_price / pow(10, token1_decimals) as token1_out_fee_usd,
                token1_in_amount * token1_price / pow(10, token1_decimals) as token1_in_amount_usd,
                token1_out_amount * token1_price / pow(10, token1_decimals) as token1_out_amount_usd,
                (token1_in_amount + token1_out_amount) / pow(10, token1_decimals) as token1_amount_native,
                (token1_in_fee + token1_out_fee) / pow(10, token1_decimals) as token1_fee_amount_native,

                case
                    when
                        (token0_in_fee_usd + token1_in_fee_usd) * 10
                        > (token0_out_fee_usd + token1_out_fee_usd)
                    then token0_out_fee_usd + token1_out_fee_usd
                    else token0_in_fee_usd + token1_in_fee_usd
                end as total_fees,
                case
                    when
                        token0_in_fee_usd > token1_in_fee_usd then
                        case
                            when token0_in_fee_usd * 10 > token1_out_fee_usd
                                then token1_fee_amount_native
                                else token0_fee_amount_native
                        end
                    else
                        case
                            when token1_in_fee_usd * 10 > token0_out_fee_usd
                                then token0_fee_amount_native
                                else token1_fee_amount_native
                        end
                end as token_fee_amount_native,
                case
                    when
                        token0_in_fee_usd > token1_in_fee_usd then
                        case
                            when token0_in_fee_usd * 10 > token1_out_fee_usd
                                then token1_symbol
                                else token0_symbol
                        end
                    else
                        case
                            when token1_in_fee_usd * 10 > token0_out_fee_usd
                                then token0_symbol
                                else token1_symbol
                        end
                end as token_fee_amount_native_symbol,
                token0_in_amount_usd + token1_in_amount_usd as total_in,
                token0_out_amount_usd + token1_out_amount_usd as total_out,
                abs(token0_in_amount_usd - token0_out_amount_usd) as abs_token0_net_amount_usd,
                abs(token1_in_amount_usd - token1_out_amount_usd) as abs_token1_net_amount_usd,
                fee_percent
            from swaps t1
            left join
                {{ chain }}_flipside.price.ez_prices_hourly t2
                on (lower(t1.token0) = lower(t2.token_address) and t2.hour = t1.hour)
            left join
                {{ chain }}_flipside.price.ez_prices_hourly t3
                on (lower(t1.token1) = lower(t3.token_address) and t3.hour = t1.hour)
            where token1_decimals > 0 and token0_decimals > 0
                and abs(
                    ln(abs(coalesce(nullif(total_in, 0), 1))) / ln(10)
                    - ln(abs(coalesce(nullif(total_out, 0), 1))) / ln(10)
                )
                < 2
                and abs( -- Necessary for filtering swaps where there is both a token0in and a token1in such as https://bscscan.com/tx/0x8c517b96974c7632627758e92675c984599b57fdced30cf00e4fef9095bf348a#eventlog#1009
                    -- more context here: https://github.com/Artemis-xyz/dbt/pull/1467
                    ln(abs(coalesce(nullif(abs_token0_net_amount_usd, 0), 1))) / ln(10)
                    - ln(abs(coalesce(nullif(abs_token1_net_amount_usd, 0), 1))) / ln(10)
                )
                < 2
        ),
        filtered_pairs as (
            select
                block_timestamp,
                tx_hash,
                event_index,
                sender,
                recipient,
                pair as pool,
                token0 as token_0,
                token0_symbol as token_0_symbol,
                token0_amount_native as token0_volume_native,
                token0_fee_amount_native,
                token1 as token_1,
                token1_symbol as token_1_symbol,
                token1_amount_native as token1_volume_native,
                token1_fee_amount_native,
                token_fee_amount_native,
                token_fee_amount_native_symbol,
                least(total_out, total_in) as trading_volume,
                total_fees as trading_fees,
                fee_percent
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
                token_0,
                token_0_symbol,
                token0_volume_native as token_0_volume_native,
                token0_fee_amount_native,
                token_1,
                token_1_symbol,
                token1_volume_native as token_1_volume_native,
                token1_fee_amount_native,
                token_fee_amount_native,
                token_fee_amount_native_symbol,
                trading_volume,
                trading_fees,
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
                and substr(t1.input, 0, 10) = '0x022c0d9f' --Swap function V2
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
        raw_gas_cost_native / 1e9 as gas_cost_native
    from events
    left join traces on 
        events.tx_hash = traces.tx_hash
        and events.pool = traces.to_address
        and events.row_number = traces.row_number
    where events.block_timestamp is not null
{% endmacro %}
