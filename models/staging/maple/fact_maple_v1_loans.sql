{{ config(materialized='table', snowflake_warehouse='MAPLE') }}

WITH pools AS (SELECT * FROM {{ ref('dim_maple_pools') }})

SELECT 
    DATE_TRUNC('day', block_timestamp) as date,
    loan as loan_id,
    pools.pool_name,
    pools.pool_id,
    amountFunded / POWER(10, pools.precision) as amount_funded
FROM
    {{ ref('fact_maple_v1_Pool_LoanFunded') }}
LEFT JOIN
    pools ON pools.v1_pool_id = contract_address
WHERE
    -- Exclude the migration Loans that were funded to bring Pools from V1->V2; December 11 2022
    DATE_TRUNC('day', block_timestamp) < DATE('2022-12-11')