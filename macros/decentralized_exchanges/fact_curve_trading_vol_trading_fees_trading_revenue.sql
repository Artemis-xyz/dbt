{% macro fact_curve_trading_vol_trading_fees_trading_revenue(_chain) %}
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
                tx_hash,
                contract_address,
                event_name,
                swap_fee,
                admin_fee,
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
                    and block_timestamp
                    >= (select max(date) + interval '1 DAY' from {{ this }})
                {% endif %}
        ),
        average_token_price_per_day as (
            select
                trunc(hour, 'day') as date,
                token_address,
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

            group by date, token_address, decimals
        ),
        with_prices as (
            select
                t1.date,
                t1.contract_address,
                tx_hash,
                t1.token_out,
                coalesce(t1.amount_out, 0) as amount_out,
                t2.decimals as token_out_decimals,
                coalesce(t2.price, t3.price) as token_out_price,
                coalesce(
                    (amount_out / pow(10, token_out_decimals)) * token_out_price, 0
                ) as amount_out_usd,
                t1.token_in,
                coalesce(t1.amount_in, 0) as amount_in,
                t3.decimals as token_in_decimals,
                coalesce(t3.price, 0) as token_in_price,
                coalesce(
                    (amount_in / pow(10, token_in_decimals)) * token_in_price, 0
                ) as amount_in_usd,
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
        )
    select
        date,
        '{{ _chain }}' as chain,
        'DeFi' as category,
        'curve' as app,
        sum(amount_out_usd) as trading_volume,
        sum(trading_fee) as fees,
        sum(revenue) as trading_revenue
    from with_prices
    where date < to_date(sysdate())
    group by date
    order by date desc
{% endmacro %}
