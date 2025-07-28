{{config(
    materialized = 'table',
    snowflake_warehouse = 'ALGORAND'
)}}

WITH 
    date_spine AS (
        SELECT 
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date >= '2019-06-11' AND date < sysdate()
    )
    , net_flows_by_address AS (
        SELECT
            parquet_raw:date::date AS date
            , parquet_raw:address::string AS address
            , parquet_raw:net_flow::float AS net_flow
        FROM {{ source('PROD_LANDING', 'raw_algorand_fact_algorand_foundation_address_flow_parquet') }}
    )
    , sparse_balances_by_address AS (
            SELECT
                date
                , address
                , SUM(net_flow) OVER (PARTITION BY address ORDER BY date) AS balance
            FROM net_flows_by_address
    )
    , address_date_pairs AS (
        WITH distinct_address AS (
            SELECT DISTINCT address FROM sparse_balances_by_address
        )
        SELECT 
            date
            , address
        FROM date_spine
        CROSS JOIN distinct_address
    )
    , dense_balances_by_address AS (
        SELECT 
            adp.date
            , adp.address
            , LAST_VALUE(sba.balance) IGNORE NULLS 
                OVER (
                    PARTITION BY adp.address 
                    ORDER BY adp.date 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS balance
        FROM address_date_pairs AS adp
        LEFT JOIN sparse_balances_by_address AS sba
            ON adp.date = sba.date
            AND adp.address = sba.address
    )

SELECT 
    date
    , SUM(balance) AS balance
FROM dense_balances_by_address
GROUP BY 1
ORDER BY 1
