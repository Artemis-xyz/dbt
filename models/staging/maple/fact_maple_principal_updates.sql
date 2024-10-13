{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH ftl_principal AS (
        with all_ftl as (
                SELECT 
                    f.block_timestamp AS date,
                    f.tx_hash,
                    f.block,
                    p.pool_name,
                    f.principalOut_ / POWER(10, p.precision) AS principal_out,
                    'FTL Outstanding Update' AS description
                FROM PC_DBT_DB.PROD.fact_maple_v2_LoanManager_PrincipalOutUpdated f
                LEFT JOIN PC_DBT_DB.PROD.dim_maple_pools p ON p.loan_manager = f.contract_address
                WHERE  date(date) > DATE('2022-12-12')
        
                UNION ALL
                SELECT
                    date, tx_hash, block, pool_name, principal_out, description
                FROM
                    fact_maple_v2_loan_manager_manual_entries
            ) 
            , intermediate as (
                select
                    date,
                    tx_hash,
                    block,
                    pool_name,
                    principal_out,
                    principal_out - LAG( principal_out, 1, 0) OVER (PARTITION BY pool_name ORDER BY date) AS delta,
                    description
                from all_ftl
            )
            SELECT * FROM intermediate
            WHERE date > date('2022-12-12')
            UNION ALL
            select
                *
            from 
                fact_maple_v2_loan_manager_manual_entries
            order by date(date) asc, delta desc
),

otl_principal AS (
    SELECT
        o.block_timestamp AS date,
        o.tx_hash,
        o.block,
        p.pool_name,
        o.principalOut_ / POWER(10, p.precision) AS principal_out,
        o.principalOut_ / POWER(10, p.precision) - LAG(o.principalOut_ / POWER(10, p.precision), 1, 0) OVER (PARTITION BY pool_name ORDER BY date) AS delta,
        'OTL Outstanding Update' AS description
    FROM {{ ref('fact_maple_v2_OpenTermLoanManager_PrincipalOutUpdated') }} o
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.open_term_loan_manager = o.contract_address
)

, all_outs as(
    SELECT * FROM ftl_principal
    UNION ALL
    SELECT * FROM otl_principal
)
SELECT * FROM all_outs
WHERE pool_name IS NOT NULL
  AND date(date) != DATE('2022-12-11')  -- Exclude events from migration day
ORDER BY date