{% macro get_pendle_tvl_for_chain_by_token(chain)%}

with 
    distinct_sy_underlyings as (
        SELECT distinct sy_address, underlying_address FROM {{ref('dim_pendle_' ~ chain  ~ '_market_metadata')}}
        {% if chain == 'arbitrum' %}
            WHERE sy_address not in ('0xc79d8a2aa6d769138e599d4dbc30569c9870a6ee', '0x318eec91f653ca72fafb038f9ad792a6bc0d644c', '0x6f14d3cd37a0647a3ee60eb2214486f8a1cddccc')
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
, cumulative as (
    select
        date(f.block_timestamp) as date
        , m.underlying_address
        , case 
            when p.symbol like '%BTC' and p.symbol <> 'WBTC' -- almost all SY tokens with BTC underlying have 8 decimals except for WBTC on Ethereum
                {% if chain == 'base' %}
                    and lower(m.sy_address) not in (lower('0x02Adf72d5D06a9C92136562Eb237C07696833a84')) -- One of the CBBTC Standard Yield token implementations has 18 decimals
                {% endif %}
                then 8 
                else 18 end as decimals
        , sum(f.amount / pow(10, decimals)) OVER (PARTITION BY m.underlying_address ORDER BY f.block_timestamp) as cum_sum
    from
        {{ref('fact_pendle_' ~ chain ~ '_deposit_redeem_txns')}} f
    left join distinct_sy_underlyings m on m.sy_address = f.sy_address
    left join prices p on p.date = date(f.block_timestamp) and p.token_address = m.underlying_address
    {% if chain == 'ethereum' %}    
        where f.sy_address not in ('0xd84e88abbe6a44a2ef9b72de9def68317d6df336', '0xab659cfa8a179fc305df3a083f1400e6a230bf15', lower('0x065347C1Dd7A23Aa043e3844B4D0746ff7715246'))
    {% endif %}
),
filled_cumulative AS (
        SELECT 
            ac.date,
            ac.underlying_address,
            LAST_VALUE(c.cum_sum IGNORE NULLS) OVER (
                PARTITION BY ac.underlying_address 
                ORDER BY ac.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_sum
        FROM all_combinations ac
        LEFT JOIN cumulative c ON ac.date = c.date AND ac.underlying_address = c.underlying_address
)
SELECT
    fc.date as date,
    '{{chain}}' AS chain,
    p.symbol as symbol,
    p.token_address as token_address,
    AVG(fc.cum_sum) AS amount_native,
    AVG(fc.cum_sum * p.price) AS amount_usd
FROM filled_cumulative fc
LEFT JOIN prices p ON p.token_address = fc.underlying_address AND fc.date = p.date
WHERE fc.cum_sum is not null and fc.cum_sum * p.price < 1e11 -- filter out outliers, highly unlikely any one token will have more than 100B USD in TVL.
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3

{% endmacro %}