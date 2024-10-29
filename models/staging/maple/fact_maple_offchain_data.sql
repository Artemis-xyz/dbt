-- fact_maple_offchain_data.sql
{{
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}


WITH prices AS (
    SELECT
        date(hour) as date,
        symbol,
        avg(price) as price
    FROM
        {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }}
    WHERE
        symbol in ('WBTC', 'INJ', 'SOL', 'STETH')
        or is_native = true
    GROUP BY 1, 2
) -- Blue Chip Secured: Collateral
, bc_collateral AS (
    SELECT
        d.date,
        btc.symbol as asset,
        d.bc_collat_btc as collateral_amount_native,
        d.bc_collat_btc * btc.price as collateral_usd
    FROM 
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date) 
        WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
        d.date,
        steth.symbol as asset,
        d.bc_collat_steth as collateral_amount_native,
        d.bc_collat_steth * steth.price as collateral_usd
    FROM 
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices steth using (date) 
        WHERE steth.symbol = 'STETH'
),

bc_totals AS (
    SELECT
        date,
        asset,
        'Secured Lending' as pool_name,
        sum(collateral_amount_native) as collat_native,
        sum(collateral_usd) as collat_usd
    FROM bc_collateral
    GROUP BY 1, 2
)

-- High Yield Secured: Collateral
, hy_collateral_by_token AS (
    SELECT
        d.date,
        sol.symbol as asset,
        d.hy_collat_sol as collateral_amount_native,
        d.hy_collat_sol * sol.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices sol using (date) 
        WHERE sol.symbol = 'SOL'
    UNION ALL
    SELECT
        d.date,
        btc.symbol as asset,
        d.hy_collat_btc as collateral_amount_native,
        d.hy_collat_btc * btc.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date) 
        WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
        d.date,
        eth.symbol as asset,
        d.hy_collat_eth as collateral_amount_native,
        d.hy_collat_eth * eth.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices eth using (date)
        WHERE eth.symbol = 'ETH'
    UNION ALL
    SELECT
        d.date,
        ftm.symbol as asset,
        d.hy_collat_ftm as collateral_amount_native,
        d.hy_collat_ftm * ftm.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices ftm using (date)
        WHERE ftm.symbol = 'FTM'
),

hy_totals AS (
    SELECT
        date,
        asset,
        'High Yield Secured Lending' as pool_name,
        sum(collateral_amount_native) as collat_native,
        sum(collateral_usd) as collat_usd
    FROM hy_collateral_by_token
    GROUP BY 1, 2
),

-- Syrup USDC: Collateral
syrup_usdc_collateral_by_token AS (
    SELECT
        d.date,
        sol.symbol as asset,
        d.syrup_usdc_collat_sol as collateral_amount_native,
        d.syrup_usdc_collat_sol * sol.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices sol using (date) 
        WHERE sol.symbol = 'SOL'
    UNION ALL
    SELECT
        d.date,
        btc.symbol as asset,
        d.syrup_usdc_collat_btc as collateral_amount_native,
        d.syrup_usdc_collat_btc * btc.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date)
        WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
        d.date,
        eth.symbol as asset,
        d.syrup_usdc_collat_eth as collateral_amount_native,
        d.syrup_usdc_collat_eth * eth.price as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices eth using (date)
        WHERE eth.symbol = 'ETH'
    UNION ALL
    SELECT
        d.date,
        'PT-USDC' as asset,
        d.syrup_usdc_collat_pt as collateral_amount_native,
        d.syrup_usdc_collat_pt * 0.986 as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
    UNION ALL
    SELECT
        d.date,
        'ORCA-LP' as asset,
        d.syrup_usdc_collat_orca as collateral_amount_native,
        d.syrup_usdc_collat_orca * 1 as collateral_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }} d
),

syrup_usdc_totals AS (
    SELECT
        date,
        asset,
        'Syrup USDC' as pool_name,
        sum(collateral_amount_native) as collat_native,
        sum(collateral_usd) as collat_usd
    FROM syrup_usdc_collateral_by_token
    GROUP BY 1, 2
),

-- Syrup USDT: Collateral
syrup_usdt_collateral_by_token AS (
   SELECT
       d.date,
       sol.symbol as asset,
       d.syrup_usdt_collat_sol as collateral_amount_native,
       d.syrup_usdt_collat_sol * sol.price as collateral_usd
   FROM                
       {{ ref('fact_maple_otc_by_day') }} d
       LEFT JOIN prices sol using (date)
       WHERE sol.symbol = 'SOL'
   UNION ALL
   SELECT
       d.date,
       btc.symbol as asset,
       d.syrup_usdt_collat_btc as collateral_amount_native,
       d.syrup_usdt_collat_btc * btc.price as collateral_usd
   FROM                
       {{ ref('fact_maple_otc_by_day') }} d
       LEFT JOIN prices btc using (date)
       WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
       d.date,
       eth.symbol as asset,
       d.syrup_usdt_collat_eth as collateral_amount_native,
       d.syrup_usdt_collat_eth * eth.price as collateral_usd
   FROM                
       {{ ref('fact_maple_otc_by_day') }} d
       LEFT JOIN prices eth using (date)
       WHERE eth.symbol = 'ETH'
   UNION ALL
   SELECT
       d.date,
       'PT-USDT' as asset,
       d.syrup_usdt_collat_pt as collateral_amount_native,
       d.syrup_usdt_collat_pt * 0.95 as collateral_usd
   FROM                
       {{ ref('fact_maple_otc_by_day') }} d
),

syrup_usdt_totals AS (
   SELECT
       date,
       asset,
       'Syrup USDT' as pool_name,
       sum(collateral_amount_native) as collat_native,
       sum(collateral_usd) as collat_usd
   FROM syrup_usdt_collateral_by_token
   GROUP BY 1, 2
),

-- Altcoin Lending: Deposits, Loans, and Collateral
alt_lending_by_token AS (
    -- SOL positions
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        sol.symbol as asset,
        'loan' as position_type,
        d.alt_loans_sol as amount_native,
        d.alt_loans_sol * sol.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices sol using (date)
        WHERE sol.symbol = 'SOL'
    UNION ALL
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        sol.symbol as asset,
        'deposit' as position_type,
        d.alt_deposits_sol as amount_native,
        d.alt_deposits_sol * sol.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices sol using (date)
        WHERE sol.symbol = 'SOL'
    
    UNION ALL
    -- BTC positions
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        btc.symbol as asset,
        'loan' as position_type,
        d.alt_loans_btc as amount_native,
        d.alt_loans_btc * btc.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date)
        WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        btc.symbol as asset,
        'deposit' as position_type,
        d.alt_deposits_btc as amount_native,
        d.alt_deposits_btc * btc.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date)
        WHERE btc.symbol = 'WBTC'
    UNION ALL
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        btc.symbol as asset,
        'collateral' as position_type,
        d.alt_collat_btc as amount_native,
        d.alt_collat_btc * btc.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices btc using (date)
        WHERE btc.symbol = 'WBTC'
    
    UNION ALL
    -- INJ positions
    SELECT
        DATE(TO_TIMESTAMP_NTZ(d.timestamp - 86399)) as date,
        inj.symbol as asset,
        'loan' as position_type,
        d.alt_loans_inj as amount_native,
        d.alt_loans_inj * inj.price as amount_usd
    FROM
        {{ ref('fact_maple_otc_by_day') }} d
        LEFT JOIN prices inj using (date)
        WHERE inj.symbol = 'INJ'
),

alt_totals AS (
    SELECT
        date,
        asset,
        'Altcoin Lending' as pool_name,
        sum(case when position_type = 'loan' then amount_usd else 0 end) as outstanding,
        sum(case when position_type = 'loan' then amount_native else 0 end) as outstanding_native,
        sum(case when position_type in ('deposit', 'collateral') or position_type = 'loan' and asset = 'INJ' then amount_usd else 0 end) as total_assets,
        sum(case when position_type in ('deposit', 'collateral') or position_type = 'loan' and asset = 'INJ' then amount_native else 0 end) as total_assets_native,
        sum(case when position_type = 'collateral' then amount_usd else 0 end) as collat_usd,
        sum(case when position_type = 'collateral' then amount_native else 0 end) as collat_native,
        sum(case when position_type in ('deposit', 'collateral') or position_type = 'loan' and asset = 'INJ' then amount_usd else 0 end) as tvl,
        sum(case when position_type in ('deposit', 'collateral') or position_type = 'loan' and asset = 'INJ' then amount_native else 0 end) as tvl_native
    FROM
        alt_lending_by_token
    WHERE
        date IS NOT NULL 
        AND date <= CURRENT_DATE
    GROUP BY 1, 2
)
-- Solana Cash Management
, solana AS (
    SELECT 
        DATE(TO_TIMESTAMP_NTZ(timestamp)) as date, 
        'USDC' as asset,  -- Source: https://maple.finance/news/maple-brings-cash-management-solution-to-solana
        'Maple Solana' as pool_name, 
        outstanding_usd as outstanding,
        outstanding_usd as outstanding_native,  
        outstanding_usd as total_assets,
        outstanding_usd as total_assets_native,
        0 as collat_native,
        0 as collat_usd,
        outstanding_usd as tvl,
        outstanding_usd as tvl_native
    FROM 
        {{ ref('fact_maple_solana_by_day') }}
    WHERE 
        DATE(TO_TIMESTAMP_NTZ(timestamp)) <= CURRENT_DATE
)

SELECT 
    date, 
    pool_name, 
    asset, 
    collat_native, 
    collat_usd as collateral, 
    NULL as outstanding, 
    NULL as outstanding_native, 
    NULL as total_assets, 
    NULL as total_assets_native, 
    NULL as tvl, 
    NULL as tvl_native
FROM bc_totals
UNION ALL
SELECT date, pool_name, asset, collat_native, collat_usd as collateral, NULL as outstanding, NULL as outstanding_native, NULL as total_assets, NULL as total_assets_native, NULL as tvl, NULL as tvl_native
FROM hy_totals
UNION ALL
SELECT date, pool_name, asset, collat_native, collat_usd as collateral, NULL as outstanding, NULL as outstanding_native, NULL as total_assets, NULL as total_assets_native, NULL as tvl, NULL as tvl_native
FROM syrup_usdc_totals
UNION ALL
SELECT date, pool_name, asset, collat_native, collat_usd as collateral, NULL as outstanding, NULL as outstanding_native, NULL as total_assets, NULL as total_assets_native, NULL as tvl, NULL as tvl_native
FROM syrup_usdt_totals
UNION ALL
-- Below, do total_assets and tvl fit the same structure by asset and pool, native, etc as collateral?
SELECT
    date, 
    pool_name, 
    asset, 
    collat_native, 
    collat_usd as collateral, 
    outstanding, 
    outstanding_native, 
    total_assets, 
    total_assets_native, 
    tvl, 
    tvl_native
FROM alt_totals
UNION ALL
SELECT date, pool_name, asset, collat_native, collat_usd as collateral, outstanding, outstanding_native, total_assets, total_assets_native, tvl, tvl_native
FROM solana