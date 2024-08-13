{% macro fact_daily_uniswap_tvl_by_token(chain) %}

with agg as (
    SELECT
        date,
        token_0_symbol as token,
        token_0_amount_native as tvl_native,
        token_0_amount_usd as tvl_usd
    FROM
        {{ ref('fact_uniswap_v3_' ~ chain ~ '_tvl_by_pool') }}
    UNION ALL
    SELECT
        date,
        token_1_symbol as token,
        token_1_amount_native as tvl_native,
        token_1_amount_usd as tvl_usd
    FROM
        {{ ref('fact_uniswap_v3_' ~ chain ~ '_tvl_by_pool') }}
    {% if chain == 'ethereum' %}
        UNION ALL
        SELECT
            date,
            token_0_symbol as token,
            token_0_amount_native as tvl_native,
            token_0_amount_usd as tvl_usd
        FROM
            {{ ref('fact_uniswap_v2_' ~ chain ~ '_tvl_by_pool') }}
        UNION ALL
        SELECT
            date,
            token_1_symbol as token,
            token_1_amount_native as tvl_native,
            token_1_amount_usd as tvl_usd
        FROM
            {{ ref('fact_uniswap_v2_' ~ chain ~ '_tvl_by_pool') }}
    {% endif %}
)
SELECT date, '{{chain}}' as chain, token, sum(tvl_native) as tvl_native, sum(tvl_usd) as tvl_usd
FROM agg
WHERE token is not null
GROUP BY 1, 2, 3

{% endmacro %}