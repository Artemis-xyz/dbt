{% macro fact_curve_tvl(_chain, _native_token) %}
    with
        tokens as (
            select
                app,
                pool_type,
                pool_address,
                name,
                symbol,
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
        coins_to_pool as (
            select pool_address, coin_0 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, coin_1 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, coin_2 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, coin_3 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, underlying_coin_0 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, underlying_coin_1 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, underlying_coin_2 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
            union all
            select pool_address, underlying_coin_3 as token_address
            from tokens
            where token_address != '0x0000000000000000000000000000000000000000'
        ),
        modified_adresses as (
            select
                block_timestamp,
                address,
                case
                    when lower(contract_address) = lower('native_token')
                    then lower('{{ _native_token }}')
                    else lower(contract_address)
                end as contract_address,
                balance_token
            from {{ ref("fact_" ~ _chain ~ "_address_balances_by_token") }}
        ),
        balances_by_block as (
            select
                block_timestamp,
                lower(pool_address) as pool_address,
                lower(contract_address) as token_address,
                balance_token,
                row_number() over (
                    partition by date(block_timestamp), token_address, pool_address
                    order by block_timestamp desc
                ) as rn
            from modified_adresses t1
            inner join
                coins_to_pool
                on lower(address) = lower(pool_address)
                and lower(contract_address) = lower(token_address)
            {% if is_incremental() %}
                where
                    block_timestamp
                    >= (select max(date) + interval '1 DAY' from {{ this }})
            {% endif %}
        ),
        token_balances_by_day as (
            select
                trunc(block_timestamp, 'day') as date,
                pool_address,
                token_address,
                balance_token
            from balances_by_block
            where rn = 1
        ),
        {% if _chain == "ethereum" %}
            coingecko_prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
        {% endif %}
        average_token_price_per_day as (
            select
                trunc(hour, 'day') as date,
                token_address,
                decimals,
                -- Pricing issue across all sources for am3CRV 
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
            {% if _chain == "ethereum" %}
                union all
                select
                    date,
                    '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as token_address,
                    0 as decimals,
                    price
                from coingecko_prices
            {% endif %}
        ),
        with_price as (
            select
                t1.date,
                t1.pool_address,
                t1.token_address,
                t1.balance_token,
                t2.decimals,
                t2.price,
                coalesce(
                    (t1.balance_token / pow(10, decimals)) * t2.price, 0
                ) as balance_usd
            from token_balances_by_day t1
            left join
                average_token_price_per_day t2
                on t1.date = t2.date
                and lower(t1.token_address) = lower(t2.token_address)
        )

    select
        date,
        '{{ _chain }}' as chain,
        'curve' as app,
        'DeFi' as category,
        sum(balance_usd) as tvl
    from with_price
    where date < to_date(sysdate())
    group by date
    order by date desc
{% endmacro %}
