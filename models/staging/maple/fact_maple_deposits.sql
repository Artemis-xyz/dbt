{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH deposits AS (
    SELECT
        d.block_timestamp AS date,
        d.tx_hash,
        d.block,
        p.pool_name,
        d.owner_ AS lender_id,
        d.shares_ / POWER(10, p.precision) AS shares,
        d.assets_ / POWER(10, p.precision) AS amount,
        'Deposit' AS description
    FROM {{ ref('fact_maple_v2_Pool_Deposit') }} d
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.pool_id = d.contract_address
)

SELECT * FROM deposits