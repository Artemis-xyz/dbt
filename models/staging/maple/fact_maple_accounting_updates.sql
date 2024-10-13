{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH ftl_accounting AS (
    SELECT 
        f.block_timestamp AS date,
        f.tx_hash,
        f.block,
        p.pool_name,
        f.accountedInterest_ / POWER(10, p.precision) AS ftl_accounted,
        f.issuanceRate_ / POWER(10, (30 + p.precision)) AS ftl_issuance_rate,
        NULL AS otl_accounted,
        NULL AS otl_issuance_rate,
        'FTL Accounting Update' AS description
    FROM {{ ref('fact_maple_v2_LoanManager_IssuanceParamsUpdated') }} f
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.loan_manager = f.contract_address
    WHERE  date(date) != DATE('2022-12-12')
),

otl_accounting AS (
    SELECT
        o.block_timestamp AS date,
        o.tx_hash,
        o.block,
        p.pool_name,
        NULL AS ftl_accounted,
        NULL AS ftl_issuance_rate,
        o.accountedInterest_ / POWER(10, p.precision) AS otl_accounted,
        o.issuanceRate_ / POWER(10, (27 + p.precision)) AS otl_issuance_rate,
        'OTL Accounting Update' AS description
    FROM {{ ref('fact_maple_v2_OpenTermLoanManager_AccountingStateUpdated') }} o
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.open_term_loan_manager = o.contract_address
),

all_accounting AS (
    SELECT * FROM ftl_accounting
    UNION ALL
    SELECT * FROM otl_accounting
)

SELECT * FROM all_accounting
WHERE pool_name IS NOT NULL
  AND date(date) != DATE('2022-12-11')  -- Exclude events from migration day
ORDER BY date