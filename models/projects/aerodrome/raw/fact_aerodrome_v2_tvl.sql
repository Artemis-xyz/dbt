{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_v2_tvl'
    )
}}

WITH pools AS (
    SELECT
        pool_address as pool,
        token0_address,
        token1_address,
        tick_spacing
    FROM {{ ref('fact_aerodrome_v2_pools') }}
), dates AS (
    SELECT
        DISTINCT DATE(hour) AS date
    FROM
        base_flipside.price.ez_prices_hourly
    WHERE
        hour > date('2024-04-01')
)
, sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address as pool,
        b.contract_address,
        decimals as decimals_adj,
        MAX_BY(balance_token / pow(10, coalesce(decimals_adj,18)), block_timestamp) AS balance_daily
    FROM
        PC_DBT_DB.PROD.fact_base_address_balances_by_token b
        LEFT JOIN base_flipside.price.ez_asset_metadata t on t.token_address = b.contract_address
    WHERE 1=1
        AND LOWER(address) in (SELECT pool FROM pools)
    GROUP BY
        1,
        2,
        3,
        4
)
, full_balances AS (
    SELECT
        d.date,
        ta.pool,
        ta.contract_address,
        COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY
                ta.pool,
                ta.contract_address
                ORDER BY
                    d.date ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            0
        ) AS balance_daily
    FROM
        dates d
    CROSS JOIN (SELECT distinct pool, contract_address FROM sparse_balances) ta 
    LEFT JOIN sparse_balances sb ON d.date = sb.date
    AND 
        ta.pool = sb.pool 
        AND ta.contract_address = sb.contract_address
)
, full_table as (
    SELECT
        fb.date,
        fb.pool,
        fb.contract_address,
        CASE 
            WHEN contract_address = 'native_token'
                THEN native_token.symbol
            ELSE p.symbol
        END AS symbol_adj,            
        fb.balance_daily as balance_daily,
        CASE 
            WHEN contract_address = 'native_token'
                THEN coalesce(native_token.price, 0)
            ELSE COALESCE(p.price, 0)
        END AS price_adj,    
        fb.balance_daily * COALESCE(price_adj, 0) AS usd_balance
    FROM
        full_balances fb
        LEFT JOIN base_flipside.price.ez_prices_hourly p ON 
                (
                    p.hour = fb.date
                    AND fb.contract_address = p.token_address
                )
        -- left join native token price
        LEFT JOIN base_flipside.price.ez_prices_hourly native_token ON
                (
                    native_token.hour = fb.date 
                    AND (lower(native_token.token_address) is null AND fb.contract_address = 'native_token')
                )
    WHERE
        symbol_adj is not null
)
SELECT
    date,
    'base' as chain,
    'v2' as version,
    pool as pool_address,
    contract_address as token_address,
    symbol_adj as token_symbol,
    SUM(balance_daily) as token_balance,
    SUM(usd_balance) as token_balance_usd
FROM
    full_table
WHERE
    USD_BALANCE > 100
    AND USD_BALANCE < 1e10 -- 10B
GROUP BY
    1
    , 2
    , 3
    , 4
    , 5
    , 6
ORDER BY
    1 DESC
