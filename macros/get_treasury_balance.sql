{% macro get_treasury_balance(chain, addresses, earliest_date)%}

{% if addresses is iterable and not addresses is string %}
    {% set addresses = addresses | map('lower') | list %}
{% else %}
    {% set addresses = addresses | lower %}
{% endif %}

WITH dates AS (
    SELECT
        DISTINCT DATE(hour) AS date
    FROM
        {{ chain }}_flipside.price.ez_prices_hourly
    WHERE
        hour > date('{{earliest_date}}')
),
tokens AS (
    SELECT
        DISTINCT LOWER(contract_address) AS token_address
        , MAX(decimals) as decimals
    FROM
        {{ chain }}_flipside.core.ez_token_transfers
    WHERE
        {% if addresses is string %}
            LOWER(to_address) = '{{ addresses }}'
        {% elif addresses | length > 1 %}
            LOWER(to_address) IN ( '{{ addresses | join("', '") }}' )
        {% endif %}
    GROUP BY 1
),
sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        user_address,
        contract_address,
        MAX_BY(balance / pow(10, decimals), block_timestamp) AS balance_daily
    FROM
        {{ chain }}_flipside.core.fact_token_balances b
        LEFT JOIN tokens t on t.token_address = b.contract_address

    WHERE
        LOWER(contract_address) IN (
            SELECT
                token_address
            FROM
                tokens
        )
        AND
        {% if addresses is string %}
            LOWER(user_address) = '{{ addresses }}'
        {% elif addresses | length > 1 %}
            LOWER(user_address) IN ( '{{ addresses | join("', '") }}' )
        {% endif %}
    GROUP BY
        1,
        2,
        3
),
full_balances AS (
    SELECT
        d.date,
        {% if addresses is string %}
        '{{ addresses }}' AS user_address,
        {% else %}
        ta.address AS user_address,
        {% endif %}
        t.token_address AS contract_address,
        COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY 
                {% if addresses is string %}
                '{{ addresses }}'
                {% else %}
                ta.address
                {% endif %},
                t.token_address
                ORDER BY
                    d.date ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            0
        ) AS balance_daily
    FROM
        dates d
        {% if addresses is string %}
            CROSS JOIN (SELECT '{{ addresses }}' AS address) ta
        {% else %}
            CROSS JOIN (
                SELECT '{{ addresses[0] }}' AS address
                {% for addr in addresses[1:] %}
                UNION ALL SELECT '{{ addr }}'
                {% endfor %}
            ) ta
        {% endif %}
        CROSS JOIN tokens t
        LEFT JOIN sparse_balances sb ON d.date = sb.date
        AND
        {% if addresses is string %}
            '{{ addresses }}' = sb.user_address
        {% else %}
            ta.address = sb.user_address
        {% endif %}
        AND t.token_address = sb.contract_address
),
daily_prices AS (
    SELECT
        DATE(hour) AS date,
        token_address,
        symbol,
        AVG(price) AS avg_daily_price,
        MAX(decimals) as decimals
    FROM
        {{ chain }}_flipside.price.ez_prices_hourly
    WHERE
        token_address IN (
            SELECT
                token_address
            FROM
                tokens
        )
    GROUP BY
        1,
        2,
        3
),
full_table as (
    SELECT
        fb.date,
        fb.user_address,
        fb.contract_address,
        dp.symbol,
        fb.balance_daily as balance_daily,
        COALESCE(dp.avg_daily_price, 0) AS avg_daily_price,
        fb.balance_daily * COALESCE(dp.avg_daily_price, 0) AS usd_balance
    FROM
        full_balances fb
        LEFT JOIN daily_prices dp ON fb.date = dp.date
        AND fb.contract_address = dp.token_address -- AND dp.decimals is not null
        -- and dp.decimals > 0
    WHERE
        symbol is not null
)
SELECT
    date,
    symbol as token,
    SUM(balance_daily) as native_balance,
    SUM(usd_balance) as usd_balance
FROM
    full_table
WHERE
    USD_BALANCE > 1
GROUP BY
    1
    , 2
ORDER BY
    1 DESC
{% endmacro %}