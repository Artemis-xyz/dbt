{% macro get_pendle_tvl_for_chain_by_token(chain)%}

with 
    distinct_sy_underlyings as (
        SELECT distinct sy_address, underlying_address FROM {{ref('dim_pendle_' ~ chain  ~ '_market_metadata')}}
        {% if chain == 'arbitrum' %}
            WHERE sy_address not in ('0xc79d8a2aa6d769138e599d4dbc30569c9870a6ee', '0x318eec91f653ca72fafb038f9ad792a6bc0d644c')
        {% endif %}
    )
, prices as (
    SELECT
        date(hour) as date
        , symbol
        , token_address
        , avg(price) as price
    FROM
    {{chain}}_flipside.price.ez_prices_hourly
    where token_address in (select underlying_address from distinct_sy_underlyings)
    and hour > date('2022-11-23')
    group by 1, 2, 3
)
, all_combinations as (
        SELECT p.date, u.underlying_address
        FROM prices p
        CROSS JOIN distinct_sy_underlyings u
)
, cum as (
    select
        date(f.block_timestamp) as date
        , m.underlying_address
        , sum(f.amount / pow(10, 18)) OVER (PARTITION BY m.underlying_address ORDER BY f.block_timestamp) as cum_sum
    from
        {{ref('fact_pendle_' ~ chain ~ '_deposit_redeem_txns')}} f
    left join distinct_sy_underlyings m on m.sy_address = f.sy_address
    {% if chain == 'ethereum' %}    
        where f.sy_address <> lower('0x065347C1Dd7A23Aa043e3844B4D0746ff7715246')
    {% endif %}
),
filled_cum AS (
        SELECT 
            ac.date,
            ac.underlying_address,
            LAST_VALUE(c.cum_sum IGNORE NULLS) OVER (
                PARTITION BY ac.underlying_address 
                ORDER BY ac.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_sum
        FROM all_combinations ac
        LEFT JOIN cum c ON ac.date = c.date AND ac.underlying_address = c.underlying_address
)
SELECT
    fc.date as date,
    '{{chain}}' AS chain,
    p.symbol as symbol,
    p.token_address as token_address,
    AVG(fc.cum_sum) AS amount_native,
    AVG(fc.cum_sum * p.price) AS amount_usd
FROM filled_cum fc
LEFT JOIN prices p ON p.token_address = fc.underlying_address AND fc.date = p.date
WHERE fc.cum_sum is not null
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3

{% endmacro %}