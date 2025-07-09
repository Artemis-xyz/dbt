{% macro fact_curve_dex_swaps(_chain) %}
    with
        dim_curve_pools as (
            select
                chain,
                app,
                pool_type,
                pool_address,
                coin_0,
                coin_1,
                coin_2,
                coin_3,
                underlying_coin_0,
                underlying_coin_1,
                underlying_coin_2,
                underlying_coin_3,
                coalesce(swap_fee, 4000000) / 1E10 as swap_fee,
                coalesce(admin_fee, 0) / 1E10 as admin_fee
            from {{ ref("dim_curve_pools_gold") }}
            {% if _chain == "avalanche" %} where chain = 'avax'
            {% else %} where chain = '{{ _chain }}'
            {% endif %}
        ),
        pool_events as (
            select
                trunc(block_timestamp, 'day') as date,
                block_timestamp,
                tx_hash,
                contract_address,
                event_name,
                swap_fee,
                admin_fee,
                decoded_log:"buyer"::string as sender,
                decoded_log:"buyer"::string as recipient, -- Curve defines these as the same
                event_index,
                decoded_log:"bought_id"::float as bought_id,
                decoded_log:"sold_id"::float as sold_id,
                iff(
                    event_name = 'TokenExchange',
                    pc_dbt_db.prod.map_token_id_to_address(
                        bought_id, t2.coin_0, t2.coin_1, t2.coin_2, t2.coin_3
                    ),
                    pc_dbt_db.prod.map_token_id_to_address(
                        bought_id,
                        t2.underlying_coin_0,
                        t2.underlying_coin_1,
                        t2.underlying_coin_2,
                        t2.underlying_coin_3
                    )
                ) as token_out_temp,
                case
                    when
                        lower(token_out_temp)
                        = lower('0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE')
                    then lower('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
                    else token_out_temp
                end as token_out,
                iff(
                    event_name = 'TokenExchange',
                    pc_dbt_db.prod.map_token_id_to_address(
                        sold_id, t2.coin_0, t2.coin_1, t2.coin_2, t2.coin_3
                    ),
                    pc_dbt_db.prod.map_token_id_to_address(
                        sold_id,
                        t2.underlying_coin_0,
                        t2.underlying_coin_1,
                        t2.underlying_coin_2,
                        t2.underlying_coin_3
                    )
                ) as token_in_temp,
                case
                    when
                        lower(token_in_temp)
                        = lower('0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE')
                    then lower('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
                    else token_in_temp
                end as token_in,
                decoded_log:"tokens_bought"::float as amount_out,
                decoded_log:"tokens_sold"::float as amount_in
            from {{ _chain }}_flipside.core.ez_decoded_event_logs t1
            inner join
                dim_curve_pools t2
                on lower(t1.contract_address) = lower(t2.pool_address)
            where
                event_name in ('TokenExchange', 'TokenExchangeUnderlying')
                {% if is_incremental() %}
                    and t1.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
                {% endif %}
        ),
        average_token_price_per_day as (
            select
                trunc(hour, 'day') as date,
                token_address,
                symbol,
                decimals,
                case
                    when
                        '{{ _chain }}' = 'polygon'
                        and lower(token_address)
                        = lower('0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171')
                        and date < '2022-11-05'
                    then 1.0
                    else avg(price)
                end as price
            from {{ _chain }}_flipside.price.ez_prices_hourly
            group by date, token_address, decimals, symbol
        ),
        with_prices as (
            select
                t1.date,
                t1.block_timestamp,
                t1.contract_address as pool,
                tx_hash,
                sender,
                recipient,
                event_index,

                t1.token_out,
                t2.symbol as token_out_symbol,
                t1.amount_out, 0 as amount_out,
                t2.decimals as token_out_decimals,
                t2.price as token_out_price,
                amount_out / pow(10, token_out_decimals) * token_out_price as amount_out_usd,
                t1.token_in,
                t3.symbol as token_in_symbol,
                t1.amount_in as amount_in,
                t3.decimals as token_in_decimals,
                t3.price as token_in_price,
                amount_in / pow(10, token_in_decimals) * token_in_price as amount_in_usd,
                amount_out_usd * t1.swap_fee as trading_fee,
                trading_fee * admin_fee as revenue
            from pool_events t1
            left join
                average_token_price_per_day t2
                on t1.date = t2.date
                and lower(t1.token_out) = lower(t2.token_address)
            left join
                average_token_price_per_day t3
                on t1.date = t3.date
                and lower(t1.token_in) = lower(t3.token_address)
        ),
        events as (
            select 
                block_timestamp,
                tx_hash,
                event_index,
                sender,
                recipient,
                pool,
                token_out,
                token_out_symbol,
                token_in,
                token_in_symbol,
                coalesce(least(amount_in_usd, amount_out_usd), 0) as trading_volume,
                trading_fee as trading_fees,
                revenue as trading_revenue,
                ROW_NUMBER() OVER (PARTITION by tx_hash, pool ORDER BY event_index) AS row_number
            from with_prices
        ),
        traces as (
            select 
                t1.*, 
                {% if _chain in ("arbitrum") %}
                    t3.gas_price_paid as gas_price,
                {% else %}
                    t3.gas_price,
                {% endif %}
                ROW_NUMBER() OVER (PARTITION by t1.tx_hash, t1.to_address ORDER BY t1.trace_index) AS row_number
            from  {{ _chain }}_flipside.core.fact_traces t1
            inner join with_prices t2 on 
                t1.tx_hash = t2.tx_hash 
                and lower(t1.to_address) = lower(t2.pool)
                and substr(t1.input, 0, 10) in ('0x3df02124', '0xa6417ed6')
            left join {{ _chain }}_flipside.core.fact_transactions t3 on t1.tx_hash = t3.tx_hash
            {% if is_incremental() %}
                where t1.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
        ),
        curve_data as (
            select
                events.block_timestamp,
                '{{ _chain }}' as chain,
                'DeFi' as category,
                'curve' as app,
                events.tx_hash,
                events.event_index,
                events.sender,
                events.recipient,
                events.pool,
                --different naming from uniswap because pool structure is different
                events.token_out,
                events.token_out_symbol,
                events.token_in,
                events.token_in_symbol,
                events.trading_volume,
                events.trading_fees,
                events.trading_revenue,
                gas_price * gas_used as raw_gas_cost_native,
                raw_gas_cost_native / 1e9 as gas_cost_native
            from events
            left join traces on 
                events.tx_hash = traces.tx_hash
                and events.pool = traces.to_address
                and events.row_number = traces.row_number
            where events.block_timestamp is not null
            {% if is_incremental() %}
                and events.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
            {% endif %}
        )
        select
            max(block_timestamp) as block_timestamp
            , max(chain) as chain
            , max(category) as category
            , max(app) as app
            , tx_hash
            , event_index
            , max(sender) as sender
            , max(recipient) as recipient
            , max(pool) as pool
            , max(token_out) as token_out
            , max(token_out_symbol) as token_out_symbol
            , max(token_in) as token_in
            , max(token_in_symbol) as token_in_symbol
            , sum(trading_volume) as trading_volume
            , sum(trading_fees) as trading_fees
            , sum(trading_revenue) as trading_revenue
            , sum(raw_gas_cost_native) as raw_gas_cost_native
            , sum(gas_cost_native) as gas_cost_native
        from curve_data
        group by tx_hash, event_index
{% endmacro %}
