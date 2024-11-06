{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH ftl_payments AS (
    SELECT
        f.block_timestamp AS date,
        f.tx_hash,
        f.block,
        p.pool_name,
        p.asset,
        f.principal_ / POWER(10, p.precision) AS principal_paid,
        f.netInterest_ / POWER(10, p.precision) AS net_interest_paid,
        'FTL Interest Payment' AS description
    FROM {{ ref('fact_maple_v2_LoanManager_FundsDistributed') }} f
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.loan_manager = f.contract_address
),

otl_payments AS (
    SELECT 
        o.block_timestamp AS date,
        o.tx_hash,
        o.block,
        p.pool_name,
        p.asset,
        o.principal_ / POWER(10, p.precision) AS principal_paid,
        (o.netInterest_ - o.platformManagementFee_ - o.delegateManagementFee_) / POWER(10, p.precision) AS net_interest_paid,
        'OTL Interest Payment' AS description
    FROM {{ ref('fact_maple_v2_OpenTermLoanManager_ClaimedFundsDistributed') }} o
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.open_term_loan_manager = o.contract_address
),

all_payments AS (
    SELECT * FROM ftl_payments
    UNION ALL
    SELECT * FROM otl_payments
)

SELECT * FROM all_payments
WHERE pool_name IS NOT NULL
ORDER BY date