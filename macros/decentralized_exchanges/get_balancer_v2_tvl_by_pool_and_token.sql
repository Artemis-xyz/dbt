{% macro get_balancer_v2_tvl_by_pool_and_token(chain) %}
    
    WITH pool_balance_changed AS (
        SELECT
            block_timestamp,
            e.pool_id,
            e.token_address,
            token_delta / pow(10, 
                coalesce(
                    p.decimals
                    ,18)
                    )
                    as token_delta
        FROM
            {{ ref('fact_balancer_v2_' ~ chain ~ '_PoolBalanceChanged_evt') }} e
            LEFT JOIN {{source((chain | upper) ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly')}} p on p.hour = date_trunc('hour', block_timestamp)
            and p.token_address = e.token_address
    ),
    pool_balance_managed AS (
        SELECT
            block_timestamp,
            decoded_log:poolId::string AS pool_id,
            decoded_log:token::string AS token_address,
            (
                decoded_log:cashDelta::number + decoded_log:managedDelta::number
            ) /    pow(10, coalesce( p.decimals,18))
                    as token_delta
        FROM
            {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
            LEFT JOIN {{source((chain | upper) ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly')}} p on p.hour = date_trunc('hour', block_timestamp)
            and p.token_address = decoded_log:token::string
        WHERE
            lower(contract_address) = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
            AND event_name = 'PoolBalanceManaged'
    ),
    swap_events as (
        SELECT
            block_timestamp,
            pool_id,
            token_in_address as token_address,
            amount_in_native as token_delta
        FROM
            {{ ref('fact_balancer_v2_' ~ chain ~ '_swaps') }}
        UNION ALL
        SELECT
            block_timestamp,
            pool_id,
            token_out_address as token_address,
            amount_out_native * -1 as token_delta
        FROM
            {{ ref('fact_balancer_v2_' ~ chain ~ '_swaps') }}
    )
    , all_deltas AS (
        SELECT
            block_timestamp,
            pool_id,
            token_address,
            token_delta
        FROM
            pool_balance_changed
        UNION ALL
        SELECT
            block_timestamp,
            pool_id,
            token_address,
            token_delta
        FROM
            pool_balance_managed
        UNION ALL
        SELECT
            block_timestamp,
            pool_id,
            token_address,
            token_delta
        FROM
            swap_events
    )
    , date_address_token_spine AS (
        SELECT
            distinct d.date,
            pt.pool_id,
            pt.token_address
        FROM
            pc_dbt_db.prod.dim_date_spine d
            CROSS JOIN (
                SELECT
                    DISTINCT pool_id,
                    token_address
                FROM
                    all_deltas
            ) pt
        WHERE
            d.date BETWEEN (
                SELECT
                    MIN(block_timestamp)
                FROM
                    all_deltas
            )
            AND CURRENT_DATE
    )
    , running_balances as (
        SELECT
            block_timestamp,
            pool_id,
            token_address,
            SUM(token_delta) OVER (
                PARTITION BY pool_id,
                token_address
                ORDER BY
                    block_timestamp ROWS UNBOUNDED PRECEDING
            ) AS running_balance
        FROM
            all_deltas
        ORDER BY
            block_timestamp
    )
    , sparse_balances as (
        SELECT
            ds.date,
            ds.pool_id,
            ds.token_address,
            max_by(running_balance, block_timestamp) as eod_balance
        FROM
            date_address_token_spine ds
            LEFT JOIN running_balances b on b.block_timestamp::date = ds.date
            and b.token_address = ds.token_address
            and b.pool_id = ds.pool_id
        GROUP BY
            1,
            2,
            3
    )
    , filled_balances as (
        SELECT
            date,
            pool_id,
            token_address,
            LAST_VALUE(eod_balance IGNORE NULLS) OVER (
                PARTITION BY pool_id,
                token_address
                ORDER BY
                    date asc ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) as native_balance
        FROM
            sparse_balances
    )
    SELECT
        date,
        pool_id,
        b.token_address,
        CASE WHEN b.token_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and date < '2021-10-08' -- No WSETH pricing data before '2021-10-08', so hardcode symbol
            THEN 'WSTETH'
            ELSE p.symbol
            END AS symbol,
        native_balance,
        CASE WHEN b.token_address = lower('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0') and date < '2021-10-08' -- No WSETH pricing data before '2021-10-08', so default to STETH
            THEN steth.price
            ELSE
            p.price
        END as price_adj,
        native_balance * price_adj as usd_balance
    FROM
        filled_balances b
        LEFT JOIN {{source((chain | upper) ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly')}} p on p.hour = date
        and p.token_address = b.token_address
        LEFT JOIN {{source((chain | upper) ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly')}} steth on steth.token_address = lower('0xae7ab96520de3a18e5e111b5eaab095312d7fe84') and steth.hour = date
    WHERE 1=1
            and usd_balance is not null and usd_balance < 10000000000 -- ten billion
            and usd_balance > 1
{% endmacro %}