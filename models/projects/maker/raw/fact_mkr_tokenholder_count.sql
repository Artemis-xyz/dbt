{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_tokenholder_count",
    )
}}

WITH filtered_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address,
        MAX_BY(balance_token / 1e18, block_timestamp) AS balance_token
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- set token contract address
    GROUP BY 1, 2
),
unique_dates AS (
    SELECT DISTINCT DATE(block_timestamp) AS date
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    where block_timestamp > '2017-12-19' -- set token contract creation date
),
addresses AS (
    SELECT
        address,
        MIN(DATE(block_timestamp)) AS first_date
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- set token contract address
    GROUP BY address
),
all_combinations AS (
    SELECT
        ud.date,
        a.address
    FROM unique_dates ud
    JOIN addresses a
    ON ud.date >= a.first_date
)
, joined_balances AS (
    SELECT
        ac.date,
        ac.address,
        fb.balance_token
    FROM all_combinations ac
    LEFT JOIN filtered_balances fb
        ON ac.date = fb.date
        AND ac.address = fb.address
)
, filled_balances AS (
    SELECT
        date,
        address,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token
    FROM joined_balances
)

select date, count(*) as tokenholder_count from filled_balances
where balance_token > 0
group by date
order by date desc


