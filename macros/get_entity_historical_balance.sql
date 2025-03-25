-- Improved version of get_treasury_balance that takes a table name as input instead of a list of addresses
-- This is useful for getting the historical balance of an entity, like a DAO Treasury, the TVL of a protocol, etc. where there are multiple
-- addresses attributed to the entity.

{% macro get_entity_historical_balance(chain, table_name, address_column, earliest_date, blacklist=(''))%}

WITH dates AS (
    SELECT
        date
    FROM
        {{ref('dim_date_spine')}}
    WHERE
        date between '{{earliest_date}}' and to_date(sysdate())
)
, sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        lower(address) as user_address,
        case 
            when contract_address = lower('0x4da27a545c0c5B758a6BA100e3a049001de870f5') -- no pricing data for stkAAVE, so default to AAVE
                then lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
                else contract_address
            end as contract_address,      
        case
            when contract_address = 'native_token' 
                then 0
            else b.decimals
        end as decimals_adj,
        MAX_BY({%if chain == 'solana' %}amount{% else %}balance_token{% endif %} / pow(10, coalesce(decimals_adj,18)), block_timestamp) AS balance_daily
    FROM
        {{ref('fact_' ~ chain ~ '_address_balances_by_token')}} b
        LEFT JOIN {{ chain }}_flipside.price.ez_asset_metadata t on t.token_address = b.contract_address
    WHERE 1=1
        AND LOWER(address) IN (SELECT distinct lower({{address_column}}) FROM {{ref(table_name)}})
        {% if blacklist is string %} and lower(contract_address) != lower('{{ blacklist }}')
        {% elif blacklist | length > 1 %} and contract_address not in {{ blacklist }} --make sure you pass in lower
        {% endif %}
    GROUP BY
        1,
        2,
        3,
        4
)
, full_balances AS (
    SELECT
        d.date,
        ta.address as user_address,
        t.token_address AS contract_address,
        COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY 
                ta.address,
                t.token_address
                ORDER BY
                    d.date ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            0
        ) AS balance_daily
    FROM
        dates d
        CROSS JOIN (SELECT distinct(lower({{address_column}})) as address FROM {{ref(table_name)}}) ta
        CROSS JOIN (SELECT distinct(contract_address) as token_address FROM sparse_balances) t
        LEFT JOIN sparse_balances sb ON d.date = sb.date
        AND ta.address = sb.user_address
        AND lower(t.token_address) = lower(sb.contract_address)
)
, full_table as (
    SELECT
        fb.date,
        fb.user_address,
        fb.contract_address,
        CASE 
            WHEN contract_address = 'native_token'
                THEN native_token.symbol
            WHEN contract_address = lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607') and fb.date < '2023-09-07' -- Flipside deosn't have pricing data for USDC.e before this date
                THEN 'USDC.e'
            ELSE p.symbol
        END AS symbol_adj,            
        fb.balance_daily as balance_daily,
        CASE 
            WHEN contract_address = 'native_token'
                THEN coalesce(native_token.price, 0)
            WHEN contract_address = lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607') and fb.date < '2023-09-07' -- Flipside deosn't have pricing data for USDC.e before this date
                THEN 1
            ELSE COALESCE(p.price, 0)
        END AS price_adj,    
        fb.balance_daily * COALESCE(price_adj, 0) AS usd_balance
    FROM
        full_balances fb
        LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p ON 
                (
                    p.hour = fb.date
                    AND lower(fb.contract_address) = lower(p.token_address)
                )
        -- left join native token price
        LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly native_token ON
                (
                    native_token.hour = fb.date 
                    AND (lower(native_token.token_address) is null AND fb.contract_address = 'native_token')
                )
    WHERE
        symbol_adj is not null
)
SELECT
    date,
    '{{ chain }}' as chain,
    contract_address,
    symbol_adj as token,
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