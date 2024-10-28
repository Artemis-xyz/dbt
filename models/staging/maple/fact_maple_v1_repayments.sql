{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH loans AS (
    SELECT DISTINCT
        l.loan_id,
        l.pool_name,
        l.pool_id,
        l.asset,
        l.precision
    FROM {{ ref('fact_maple_v1_loans') }} l
),

v1_payments AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        contract_address as loan_id,
        loans.pool_name,
        loans.pool_id,
        loans.asset,
        interestPaid / POWER(10, loans.precision) as gross_interest_paid,
        principalPaid / POWER(10, loans.precision) as principal_paid
    FROM
        {{ ref('fact_maple_v1_Loan_evt_PaymentMade') }}
    LEFT JOIN
        loans ON loans.loan_id = contract_address
),

payments AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as date,
        contract_address as loan_id,
        loans.pool_name,
        loans.pool_id,
        loans.asset,
        interestPaid_ / POWER(10, loans.precision) as gross_interest_paid,
        principalPaid_ / POWER(10, loans.precision) as principal_paid
    FROM
        {{ ref('fact_maple_v1_FixedTermLoan_evt_PaymentMade') }}
    LEFT JOIN 
        loans ON loans.loan_id = contract_address
)

SELECT * FROM v1_payments
UNION ALL
-- For the newer versions of Loan, filter out anything after the V2 migration
SELECT * FROM payments WHERE date < DATE('2022-12-11')