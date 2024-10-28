{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

SELECT
    date(date) as date, 
    tx_hash,
    block,
    description, 
    pool_name,
    asset, 
    amount, 
    shares, 
    interest, 
    principal,
    -- Assets (idle cash) will be a running sum of the amount column; by Pool
    -- Basically the cash in the Pool is always changing as deposits come in, loans are issued, payments are made, etc.
    -- Subtract principal since if principal decreased, that's an increase to Pool cash
    SUM(amount + interest - principal) OVER (PARTITION BY pool_name ORDER BY block) as assets,
    SUM(shares) OVER (PARTITION BY pool_name ORDER BY block) as pool_shares,
    ftl_outstanding + otl_outstanding as outstanding,
    ftl_accounted + otl_accounted as accounted,
    ftl_outstanding, 
    ftl_accounted, 
    ftl_issuance_rate, 
    ftl_domain_start,
    otl_outstanding, 
    otl_accounted, 
    otl_issuance_rate, 
    otl_domain_start
FROM {{ ref('fact_maple_all_events') }}