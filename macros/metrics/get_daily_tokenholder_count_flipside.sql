{ % macro get_daily_tokenholder_count_flipside(chain, token_address) %}

WITH filtered_balances AS (
    SELECT
        DATE(b.block_timestamp) AS date,
        b.user_address as address,
        MAX_BY(b.balance/ m.decimals, b.block_timestamp) AS balance_token
    FROM {{chain}}_flipside.core.fact_token_balances b
    LEFT JOIN {{chain}}_flipside.price.ez_asset_metadata m ON m.token_address = b.contract_address
    WHERE b.contract_address = lower({{ token_address }}) -- set token contract address
    GROUP BY 1, 2
)
, unique_dates AS (
    SELECT DISTINCT DATE(hour) AS date
    FROM {{chain}}_flipside.price.ez_prices_hourly
    WHERE hour > (SELECT MIN(date) FROM filtered_balances)
)
, addresses AS (
    SELECT
        user_address as address,
        MIN(DATE(block_timestamp)) AS first_date
    FROM {{chain}}_flipside.core.fact_token_balances
    WHERE contract_address = lower({{ token_address }})
    GROUP BY user_address
)
, all_combinations AS (
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

{ % endmacro %}