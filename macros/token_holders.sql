{% macro token_holders(chain, contract_address, creation_date) %}
WITH filtered_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address,
        MAX_BY(balance_token / 1e18, block_timestamp) AS balance_token
    FROM {{ref('fact_'~chain~'_address_balances_by_token')}}
    WHERE contract_address = lower('{{contract_address}}')
    GROUP BY 1, 2
),
unique_dates AS (
    SELECT DISTINCT DATE(block_timestamp) AS date
    FROM {{ref('fact_'~chain~'_address_balances_by_token')}}
    where block_timestamp > '{{creation_date}}'
),
addresses AS (
    SELECT
        address,
        MIN(DATE(block_timestamp)) AS first_date
    FROM {{ref('fact_'~chain~'_address_balances_by_token')}}
    WHERE contract_address = lower('{{contract_address}}')
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

select 
    date
    , '{{ chain }}' as chain
    , count(*) as token_holder_count 
from filled_balances
where balance_token > 0
group by date
order by date desc
{% endmacro %}


