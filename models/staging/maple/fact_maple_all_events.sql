{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH deposits AS (
    SELECT
        date,
        tx_hash,
        block,
        'Deposit' as description,
        pool_name,
        amount,
        shares,
        NULL as interest,
        NULL as principal,
        NULL as ftl_outstanding,
        NULL as ftl_accounted,
        NULL as ftl_issuance_rate,
        NULL as ftl_domain_start,
        NULL as otl_outstanding,
        NULL as otl_accounted,
        NULL as otl_issuance_rate,
        NULL as otl_domain_start
    FROM {{ ref('fact_maple_deposits') }}
),

withdrawals AS (
    SELECT
        date,
        tx_hash,
        block,
        'Withdrawal' as description,
        pool_name,
        amount * -1 as amount,
        shares * -1 as shares,
        NULL as interest,
        NULL as principal,
        NULL as ftl_outstanding,
        NULL as ftl_accounted,
        NULL as ftl_issuance_rate,
        NULL as ftl_domain_start,
        NULL as otl_outstanding,
        NULL as otl_accounted,
        NULL as otl_issuance_rate,
        NULL as otl_domain_start
    FROM {{ ref('fact_maple_withdrawals') }}
),

payments AS (
    SELECT
        date,
        tx_hash,
        block,
        description,
        pool_name,
        NULL as amount,
        NULL as shares,
        net_interest_paid as interest,
        NULL as principal,
        NULL as ftl_outstanding,
        NULL as ftl_accounted,
        NULL as ftl_issuance_rate,
        NULL as ftl_domain_start,
        NULL as otl_outstanding,
        NULL as otl_accounted,
        NULL as otl_issuance_rate,
        NULL as otl_domain_start
    FROM {{ ref('fact_maple_payments') }}
),

accounting AS (
    SELECT
        date,
        tx_hash,
        block,
        description,
        pool_name,
        NULL as amount,
        NULL as shares,
        NULL as interest,
        NULL as principal,
        NULL as ftl_outstanding,
        ftl_accounted,
        ftl_issuance_rate,
        CASE WHEN description = 'FTL Accounting Update' THEN date ELSE NULL END AS ftl_domain_start,
        NULL as otl_outstanding,
        otl_accounted,
        otl_issuance_rate,
        CASE WHEN description = 'OTL Accounting Update' THEN date ELSE NULL END AS otl_domain_start
    FROM {{ ref('fact_maple_accounting_updates') }}
),

outstanding AS (
    SELECT
        date,
        tx_hash,
        block,
        description,
        pool_name,
        NULL as amount,
        NULL as shares,
        NULL as interest,
        delta as principal,
        CASE WHEN description = 'FTL Outstanding Update' THEN principal_out ELSE NULL END AS ftl_outstanding,
        NULL as ftl_accounted,
        NULL as ftl_issuance_rate,
        NULL as ftl_domain_start,
        CASE WHEN description = 'OTL Outstanding Update' THEN principal_out ELSE NULL END AS otl_outstanding,
        NULL as otl_accounted,
        NULL as otl_issuance_rate,
        NULL as otl_domain_start
    FROM {{ ref('fact_maple_principal_updates') }}
),

outstanding_accounting AS (
    SELECT
        outs.date,
        outs.tx_hash,
        outs.block,
        CASE
            WHEN outs.principal > 0 AND outs.description = 'FTL Outstanding Update' THEN 'FTL Issuance'
            WHEN outs.principal > 0 AND outs.description = 'OTL Outstanding Update' THEN 'OTL Issuance'
            WHEN outs.principal < 0 AND outs.description = 'FTL Outstanding Update' THEN 'FTL Paid Down'
            WHEN outs.principal < 0 AND outs.description = 'OTL Outstanding Update' THEN 'OTL Paid Down'
            WHEN outs.principal = 0 AND outs.description = 'FTL Outstanding Update' THEN 'FTL Refinance'
            WHEN outs.principal = 0 AND outs.description = 'OTL Outstanding Update' THEN 'OTL Refinance'
            ELSE 'Accounting Update'
        END AS description,
        outs.pool_name,
        outs.amount,
        outs.shares,
        CASE
            WHEN outs.principal = 0 AND outs.description = 'FTL Outstanding Update' THEN NULL
            ELSE p.interest
        END AS interest,
        outs.principal,
        outs.ftl_outstanding,
        a.ftl_accounted,
        a.ftl_issuance_rate,
        a.ftl_domain_start,
        outs.otl_outstanding,
        a.otl_accounted,
        a.otl_issuance_rate,
        a.otl_domain_start
    FROM outstanding outs
    LEFT JOIN accounting a ON a.block = outs.block AND a.pool_name = outs.pool_name
    LEFT JOIN payments p ON p.block = outs.block AND p.pool_name = outs.pool_name
),

interest_payments AS (
    SELECT
        p.date,
        p.tx_hash,
        p.block,
        p.description,
        p.pool_name,
        p.amount,
        p.shares,
        p.interest,
        outs.principal,
        outs.ftl_outstanding,
        a.ftl_accounted,
        a.ftl_issuance_rate,
        a.ftl_domain_start,
        outs.otl_outstanding,
        a.otl_accounted,
        a.otl_issuance_rate,
        a.otl_domain_start
    FROM payments p
    LEFT JOIN outstanding outs ON p.block = outs.block AND p.pool_name = outs.pool_name
    LEFT JOIN accounting a ON a.block = p.block AND a.pool_name = p.pool_name
    WHERE outs.principal IS NULL
),

joined AS (
    SELECT * FROM outstanding_accounting
    UNION ALL
    SELECT * FROM interest_payments
),

all_events AS (
    SELECT * FROM deposits
    UNION ALL
    SELECT * FROM withdrawals
    UNION ALL
    SELECT * FROM joined
),

all_events_migration AS (
    SELECT
        date,
        tx_hash,
        block,
        description,
        pool_name,
        CASE
            WHEN block = 16164991 AND pool_name = 'M11 Credit USDC1' THEN 8765965.068493
            WHEN block = 16164991 AND pool_name = 'Orthogonal Credit USDC1' THEN 16944059.896081
            WHEN block = 16164991 AND pool_name = 'M11 Credit USDC2' THEN 1.24615 + 615006.54
            WHEN (block = 16178669 OR block = 16178695 OR block = 16178705) AND pool_name = 'M11 Credit USDC2' THEN 0
            WHEN block = 16164991 AND pool_name = 'M11 Credit WETH' THEN 1395.6268730806437 + 64.07
            WHEN (block = 16178815 OR block = 16178824 OR block = 16178830) AND pool_name = 'M11 Credit WETH' THEN 0
            ELSE amount
        END AS amount,
        shares,
        interest,
        principal,
        ftl_outstanding,
        ftl_accounted,
        ftl_issuance_rate,
        ftl_domain_start,
        otl_outstanding,
        otl_accounted,
        otl_issuance_rate,
        otl_domain_start
    FROM all_events
)

SELECT
    date,
    tx_hash,
    block,
    description,
    pool_name,
    COALESCE(amount, 0) as amount,
    COALESCE(shares, 0) as shares,
    COALESCE(interest, 0) as interest,
    COALESCE(principal, 0) as principal,
    COALESCE(LAST_VALUE(ftl_outstanding) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as ftl_outstanding,
    COALESCE(LAST_VALUE(ftl_accounted) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as ftl_accounted,
    COALESCE(LAST_VALUE(ftl_issuance_rate) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as ftl_issuance_rate,
    COALESCE(LAST_VALUE(DATE_PART(EPOCH_SECONDS, ftl_domain_start)) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as ftl_domain_start,
    otl_outstanding,
    COALESCE(LAST_VALUE(otl_outstanding) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as otl_outstanding_last,
    COALESCE(LAST_VALUE(otl_accounted) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as otl_accounted,
    COALESCE(LAST_VALUE(otl_issuance_rate) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as otl_issuance_rate,
    COALESCE(LAST_VALUE(DATE_PART(EPOCH_SECONDS, otl_domain_start)) IGNORE NULLS OVER (
        PARTITION BY pool_name 
        ORDER BY block
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 0) as otl_domain_start
FROM all_events_migration
WHERE pool_name IS NOT NULL
ORDER BY block DESC