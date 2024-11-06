{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH dates AS (
    SELECT * FROM {{ ref('dim_maple_dates') }}
),

pool_ids AS (
    SELECT DISTINCT pool_id, asset, pool_name, block_activated 
    FROM {{ ref('dim_maple_pools') }}
    WHERE pool_name NOT IN ('Celsius WETH Pool', 'Alameda Research - USDC', 'BlockTower Capital - USDC01')
),

dates_pools AS (
    SELECT
        d.date, 
        -- For the UNIX timestamp, we want the LAST timestamp of the day, so add 86399 seconds
        DATE_PART('epoch', d.date) + 86399 as last_ts,
        d.last_block, 
        p.pool_name,
        p.asset
    FROM dates d 
    CROSS JOIN pool_ids p
    -- Only add Pool to each date if it had already been activated
    WHERE d.last_block >= p.block_activated
),

-- From the table of all Pool states, pull only the latest each day, by Pool
latest_states_only AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY pool_name, date ORDER BY date DESC) as row_num
    FROM {{ ref('fact_maple_pool_state_history') }}
),

-- Fill in Pool values for every day where there was not an update
states AS (
    SELECT 
        d.date, d.last_ts, d.last_block, d.pool_name, d.asset,
        LAST_VALUE(s.assets) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as assets,
        LAST_VALUE(s.pool_shares) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as pool_shares,
        LAST_VALUE(s.outstanding) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as outstanding,
        LAST_VALUE(s.accounted) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as accounted,
        LAST_VALUE(s.ftl_issuance_rate) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as ftl_issuance_rate,
        LAST_VALUE(s.ftl_domain_start) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as ftl_domain_start,
        LAST_VALUE(s.otl_issuance_rate) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as otl_issuance_rate,
        LAST_VALUE(s.otl_domain_start) IGNORE NULLS OVER (
            PARTITION BY d.pool_name 
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as otl_domain_start
    FROM dates_pools d
    LEFT JOIN latest_states_only s
    ON s.date = d.date AND s.pool_name = d.pool_name AND s.row_num = 1 
),

-- Prep some columns like accrued interest that will be needed for total assets
prep_states AS (
    SELECT
        date, last_ts, last_block, pool_name, asset, assets, pool_shares, outstanding, 
        -- Balance is the amount of assets in the Pool that is currently earning interest; outstanding + idle cash
        assets + outstanding as balance,
        accounted,
        -- Annualize interest from both FTL and OTL Loan Managers; using 365 days/year and 86400 seconds/day
        ftl_issuance_rate,
        ftl_issuance_rate * 86400 * 365 as ftl_ann_interest,
        ftl_domain_start,
        (last_ts - ftl_domain_start) * ftl_issuance_rate as ftl_accrued,
        otl_issuance_rate,
        otl_issuance_rate * 86400 * 365 as otl_ann_interest,
        otl_domain_start,
        (last_ts - otl_domain_start) * otl_issuance_rate as otl_accrued
    FROM states
),

-- Calculate total assets, exchange rate, current net APY, and utilization
all_states AS (
    SELECT
        *,
        ftl_accrued + otl_accrued as total_accrued,
        assets + outstanding + accounted + (ftl_accrued + otl_accrued) as total_assets,
        (assets + outstanding + accounted + (ftl_accrued + otl_accrued)) / NULLIF(pool_shares, 0) as exch_rate,
        CASE 
            WHEN pool_name = 'High Yield Secured Lending' THEN (ftl_ann_interest + otl_ann_interest) / NULLIF(balance, 0) + 0.08
            WHEN pool_name = 'Syrup USDC' OR pool_name = 'Syrup USDT' THEN (ftl_ann_interest + otl_ann_interest) / NULLIF(balance, 0) + 0.066
            ELSE (ftl_ann_interest + otl_ann_interest) / NULLIF(balance, 0) 
        END AS spot_apy,
        outstanding / NULLIF(balance, 0) as utilization
    FROM prep_states
),

-- Add trailing averages; e.g. 30 day average APY, lifetime APY, etc.
all_states_trailing AS (
    SELECT
        *,
        AVG(CASE WHEN spot_apy > 0 THEN spot_apy ELSE NULL END) OVER (PARTITION BY pool_name ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as apy_trailing_30,
        AVG(CASE WHEN spot_apy > 0 THEN spot_apy ELSE NULL END) OVER (PARTITION BY pool_name ORDER BY date) as lifetime_apy
    FROM all_states
)

SELECT * FROM all_states_trailing 
WHERE pool_name IN ('Syrup USDC', 'Syrup USDT', 'Secured Lending', 'High Yield Secured Lending', 'Maple Cash USDC1', 'High Yield Corporate USDC', 'AQRU Receivables Financing')
