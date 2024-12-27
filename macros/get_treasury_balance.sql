{% macro get_treasury_balance(chain, addresses, earliest_date, blacklist=(''))%}
-- Returns the balance of a set of addresses (belonging to an entity such as a treasury) on a given chain aggregated by date and token

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
sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address as user_address,
        case 
            when contract_address = lower('0x4da27a545c0c5B758a6BA100e3a049001de870f5') -- no pricing data for stkAAVE, so default to AAVE
                then lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
                else contract_address
            end as contract_address,        
        MAX_BY(balance_token / pow(10, coalesce(decimals,18)), block_timestamp) AS balance_daily
    FROM
        {{ref('fact_' ~ chain ~ '_address_balances_by_token')}} b
        LEFT JOIN {{ chain }}_flipside.price.ez_asset_metadata t on t.token_address = b.contract_address
    WHERE 1=1
        {% if addresses is string %}
            AND LOWER(address) = '{{ addresses }}'
        {% elif addresses | length > 1 %}
            AND LOWER(address) IN ( '{{ addresses | join("', '") }}' )
        {% endif %}
        {% if blacklist is string %} and lower(contract_address) != lower('{{ blacklist }}')
        {% elif blacklist | length > 1 %} and contract_address not in {{ blacklist }} --make sure you pass in lower
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
        CROSS JOIN (SELECT distinct(contract_address) as token_address FROM sparse_balances) t
        LEFT JOIN sparse_balances sb ON d.date = sb.date
        AND
        {% if addresses is string %}
            '{{ addresses }}' = sb.user_address
        {% else %}
            ta.address = sb.user_address
        {% endif %}
        AND t.token_address = sb.contract_address
),
full_table as (
    SELECT
        fb.date,
        fb.user_address,
        fb.contract_address,
        p.symbol,
        fb.balance_daily as balance_daily,
        COALESCE(p.price, 0) AS price,
        fb.balance_daily * COALESCE(p.price, 0) AS usd_balance
    FROM
        full_balances fb
        LEFT JOIN ethereum_flipside.price.ez_prices_hourly p ON p.hour = fb.date
        AND fb.contract_address = p.token_address -- AND dp.decimals is not null
        -- and dp.decimals > 0
    WHERE
        symbol is not null
)
SELECT
    date,
    '{{ chain }}' as chain,
    contract_address,
    symbol as token,
    SUM(balance_daily) as native_balance,
    SUM(usd_balance) as usd_balance
FROM
    full_table
WHERE
    USD_BALANCE > 100
GROUP BY
    1
    , 2
    , 3
    , 4
ORDER BY
    1 DESC
{% endmacro %}