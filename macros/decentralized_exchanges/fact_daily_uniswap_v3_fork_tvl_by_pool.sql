{% macro fact_daily_uniswap_v3_fork_tvl_by_pool(token_address, chain, app) %}
    with recursive
        pools as (
            select
                decoded_log:"pool"::string as pool,
                decoded_log:"token0"::string as token0,
                decoded_log:"token1"::string as token1,
                decoded_log:"fee"::float as fee
            from {{ chain }}_flipside.core.ez_decoded_event_logs
            where
                contract_address = lower('{{ token_address }}')
                and event_name = 'PoolCreated'
        ),
        all_pool_events as (
            select
                t2.*,
                t1.tx_hash,
                t1.block_number,
                t1.decoded_log,
                t1.block_timestamp,
                t1.event_name
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
            inner join pools t2 on lower(t1.contract_address) = lower(t2.pool)
            where
                t1.event_name in ('Mint', 'Burn', 'Swap')
                {% if is_incremental() %}
                    and block_timestamp >= (select max(date) from {{ this }})
                {% endif %}
        ),
        mint_and_burn_and_swap_liquidity as (
            select
                trunc(block_timestamp, 'day') as date,
                tx_hash,
                event_name,
                pool,
                token0,
                case
                    when event_name = 'Burn'
                    then - decoded_log:"amount0"::float
                    else decoded_log:"amount0"::float
                end as token0_amount,
                token1,
                case
                    when event_name = 'Burn'
                    then - decoded_log:"amount1"::float
                    else decoded_log:"amount1"::float
                end as token1_amount
            from all_pool_events
        ),
        adjusted_mint_and_burn_and_swap_liquidity as (
            select
                t1.date,
                t1.tx_hash,
                t1.event_name,
                t1.pool,

                t1.token0,
                t1.token0_amount,
                t2.decimals as token0_decimals,
                t1.token0_amount / pow(10, token0_decimals) as token0_amount_adj,

                t1.token1,
                t1.token1_amount,
                t3.decimals as token1_decimals,
                t1.token1_amount / pow(10, token1_decimals) as token1_amount_adj
            from mint_and_burn_and_swap_liquidity t1
            left join
                {{ chain }}_flipside.core.dim_contracts t2
                on lower(t1.token0) = lower(t2.address)
            left join
                {{ chain }}_flipside.core.dim_contracts t3
                on lower(t1.token1) = lower(t3.address)
            where token0_decimals != 0 and token1_decimals != 0
        ),
        token_changes_per_pool_per_day as (
            select
                date,
                pool,
                token0,
                sum(token0_amount_adj) as token0_amount_per_day,
                token1,
                sum(token1_amount_adj) as token1_amount_per_day
            from adjusted_mint_and_burn_and_swap_liquidity
            group by date, pool, token0, token1
        ),
        min_date as (
            select min(date) as date, pool, token0, token1
            from token_changes_per_pool_per_day
            group by pool, token0, token1
        ),
        date_range as (
            select
                date,
                pool,
                token0,
                0 as token0_amount_per_day,
                token1,
                0 as token1_amount_per_day
            from min_date
            union all
            select
                dateadd(day, 1, date),
                pool,
                token0,
                token0_amount_per_day,
                token1,
                token1_amount_per_day
            from date_range
            where date < to_date(sysdate())
        ),
        token_changes_per_pool_per_day_every_day as (
            select *
            from date_range
            union all
            select *
            from token_changes_per_pool_per_day
        ),
        token_cumulative_per_day_raw as (
            select
                date,
                pool,
                token0,
                sum(token0_amount_per_day) over (
                    partition by pool order by date
                ) as token0_cumulative,
                token1,
                sum(token1_amount_per_day) over (
                    partition by pool order by date
                ) as token1_cumulative
            from token_changes_per_pool_per_day_every_day
        ),
        token_cumulative_per_day as (
            select *
            from token_cumulative_per_day_raw
            group by date, pool, token0, token0_cumulative, token1, token1_cumulative
        ),
        average_token_price_per_day as (
            select trunc(hour, 'day') as date, token_address, symbol, avg(price) as price
            from {{ chain }}_flipside.price.ez_hourly_token_prices
            group by date, token_address, symbol
        ),
        with_price as (
            select
                t1.date,
                pool,
                token0,
                t2.symbol as token0_symbol,
                coalesce(t2.price, 0) as token0_price,
                coalesce(token0_cumulative, 0) as token0_cumulative,
                coalesce(token0_cumulative * token0_price, 0) as token0_amount_usd,
                token1,
                t3.symbol as token1_symbol,
                coalesce(t3.price, 0) as token1_price,
                coalesce(token1_cumulative, 0) as token1_cumulative,
                coalesce(token1_cumulative * token1_price, 0) as token1_amount_usd
            from token_cumulative_per_day t1
            left join
                average_token_price_per_day t2
                on t1.date = t2.date
                and lower(t1.token0) = lower(t2.token_address)
            left join
                average_token_price_per_day t3
                on t1.date = t3.date
                and lower(t1.token1) = lower(t3.token_address)
        ),
        viable_pools as (
            select date, pool, token0, token0_symbol, token1, token1_symbol, token0_amount_usd + token1_amount_usd as pool_tvl
            from with_price
            where
                abs(
                    ln(abs(coalesce(nullif(token0_amount_usd, 0), 1))) / ln(10)
                    - ln(abs(coalesce(nullif(token1_amount_usd, 0), 1))) / ln(10)
                )
                < 1
        )
    select 
        date, 
        '{{ chain }}' as chain, 
        '{{ app }}' as app, 
        'DeFi' as category, 
        pool,
        token0 as token_0,
        token0_symbol as token_0_symbol,
        token1 as token_1,
        token1_symbol as token_1_symbol,
        sum(pool_tvl) as tvl
    from viable_pools
    where date is not null
    group by date, pool, token_0, token_0_symbol, token_1, token_1_symbol
{% endmacro %}