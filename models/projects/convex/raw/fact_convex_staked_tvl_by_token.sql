{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'CONVEX',
        database = 'CONVEX',
        schema = 'raw',
        alias = 'fact_convex_staked_tvl_by_token'
    )
}}

with lp_to_gauge as (
    SELECT
        lptoken as lp_token,
        gauge
    FROM
        {{ ref('fact_convex_pools') }}
),
eod_address_token_balances as (
    SELECT
        block_timestamp::date as date,
        address,
        lp.lp_token as contract_address,
        max_by(balance_token, block_timestamp) as eod_balance
    FROM
        {{ ref('fact_ethereum_address_balances_by_token') }}
        LEFT JOIN lp_to_gauge lp ON lp.gauge = contract_address
    WHERE
        lower(address) = lower('0x989aeb4d175e16225e39e87d0d97a3360524ad80') -- Voter Proxy Address
        AND contract_address in (
            SELECT
                gauge
            FROM
                lp_to_gauge
        )
    GROUP BY
        1,
        2,
        3
),
distinct_address_tokens as (
    SELECT
        DISTINCT address,
        contract_address
    FROM
        eod_address_token_balances
),
date_address_token_spine as (
    SELECT
        DISTINCT ds.date,
        cp.address,
        cp.contract_address
    FROM
        {{ ref('dim_date_spine') }} ds
        CROSS JOIN distinct_address_tokens cp
    WHERE
        ds.date between '2020-02-29'
        and to_date(sysdate())
),
sparse_balances as (
    SELECT
        dats.date,
        dats.address,
        dats.contract_address,
        b.eod_balance
    FROM
        date_address_token_spine dats
        LEFT JOIN eod_address_token_balances b using(date, address, contract_address)
),
filled_balances as (
    SELECT
        date,
        address,
        contract_address,
        COALESCE(
            eod_balance,
            LAST_VALUE(eod_balance IGNORE NULLS) OVER (
                PARTITION BY address,
                contract_address
                ORDER BY
                    date ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            )
        ) AS daily_balance
    FROM
        sparse_balances
)
SELECT
    b.date,
    b.address,
    b.contract_address as token_address,
    cp.coin_0,
    cp.coin_1,
    c.name,
    b.daily_balance / POW(10, 18) as balance_native
FROM
    filled_balances b
    LEFT JOIN {{ source('ETHEREUM_FLIPSIDE', 'dim_contracts') }} c on c.address = b.contract_address
    LEFT JOIN {{ ref('dim_curve_pools') }} cp on lower(cp.token) = lower(b.contract_address)
WHERE
    b.daily_balance IS NOT NULL