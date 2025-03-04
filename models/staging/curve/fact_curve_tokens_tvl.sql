{{
    config(
        materialized='table',
        snowflake_warehouse='CURVE',
    )
}}

-- Used for Convex for calculating TVL since we need price of all Curve related tokens (including LP tokens)
-- This table is different from fact_curve_{chain}_pools_tvl.sql because it includes all Curve related tokens including NG pools

with curve_pools as (
    SELECT distinct(address) as address FROM (
        SELECT lower(token) as address FROM {{ ref('dim_curve_pools') }} curve
        UNION ALL
        SELECT lower(pool_address) as address FROM {{ ref('dim_curve_pools') }} curve
        UNION ALL
        SELECT lower(contract_address) as address FROM {{ ref('dim_curve_stable_ng_pools') }}
        UNION ALL 
        SELECT lower(lptoken) as address FROM {{ ref('fact_convex_pools') }}
    )
)
, eod_address_token_balances as (
    SELECT
        block_timestamp::date as date
        , address
        , contract_address
        , max_by(balance_token, block_timestamp) as eod_balance
    FROM {{ ref('fact_ethereum_address_balances_by_token') }}
    WHERE 1=1
        AND address in (
            SELECT address FROM curve_pools
            )
    GROUP BY
        1
        , 2
        , 3
)
, distinct_address_tokens as (
    SELECT
        DISTINCT
            address,
            contract_address
    FROM
        eod_address_token_balances
)
, date_address_token_spine as (
    SELECT
        DISTINCT
        ds.date,
        cp.address,
        cp.contract_address
    FROM {{ ref('dim_date_spine') }} ds
    CROSS JOIN distinct_address_tokens cp
    WHERE ds.date between '2020-02-29' and to_date(sysdate())
)
, sparse_balances as (
    SELECT
        dats.date,
        dats.address,
        dats.contract_address,
        b.eod_balance
    FROM
        date_address_token_spine dats
    LEFT JOIN eod_address_token_balances b using(date, address, contract_address)
)
, filled_balances as (
    SELECT
        date,
        address,
        contract_address,
        COALESCE(eod_balance,
            LAST_VALUE(eod_balance IGNORE NULLS) OVER (
                PARTITION BY address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW 
                )
            )
            AS daily_balance
    FROM
        sparse_balances
)
SELECT
    date,
    address,
    SUM(daily_balance/POW(10, COALESCE(p.decimals,18)) * p.price) as tvl
FROM
    filled_balances
LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p on (
        p.hour = date 
        AND lower(p.token_address) = lower(contract_address)
    )
GROUP BY
    1
    , 2