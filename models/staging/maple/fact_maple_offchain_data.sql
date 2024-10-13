-- fact_maple_offchain_data.sql
{{
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH btc AS (SELECT price FROM {{ ref('fact_token_prices') }} WHERE symbol = 'WBTC' ORDER BY date DESC LIMIT 1),
eth AS (SELECT price FROM {{ ref('fact_token_prices') }} WHERE symbol = 'ETH' ORDER BY date DESC LIMIT 1),
steth AS (SELECT price FROM {{ ref('fact_token_prices') }} WHERE symbol = 'stETH' ORDER BY date DESC LIMIT 1),
sol AS (SELECT price FROM {{ ref('fact_token_prices') }} WHERE symbol = 'SOL' ORDER BY date DESC LIMIT 1),
inj AS (SELECT price FROM {{ ref('fact_token_prices') }} WHERE symbol = 'INJ' ORDER BY date DESC LIMIT 1),

-- Blue Chip Secured: Collateral
bc_collateral AS (
    SELECT 
        date, 
        timestamp, 
        bc_collat_btc, 
        bc_collat_btc * btc.price as bc_btc_collat_usd,
        bc_collat_steth,
        bc_collat_steth * steth.price as steth_collat_usd
    FROM 
        {{ ref('fact_maple_otc_by_day') }}, btc, steth
),

bc_totals AS (
    SELECT
        date,
        timestamp,
        'Secured Lending' as pool_name,
        bc_btc_collat_usd + steth_collat_usd as collat_usd
    FROM bc_collateral
),

-- High Yield Secured: Collateral
hy_collateral AS (
    SELECT
        date,
        timestamp,
        'High Yield Secured Lending' as pool_name,
        hy_collat_sol,
        hy_collat_sol * sol.price as hy_sol_collat_usd,
        hy_collat_btc,
        hy_collat_btc * btc.price as hy_btc_collat_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }}, sol, btc
),

hy_totals AS (
    SELECT
        date,
        timestamp,
        pool_name,
        hy_sol_collat_usd + hy_btc_collat_usd as collat_usd
    FROM hy_collateral
),

-- Syrup USDC: Collateral
syrup_usdc_collateral AS (
    SELECT
        date,
        timestamp,
        'Syrup USDC' as pool_name,
        syrup_usdc_collat_sol,
        syrup_usdc_collat_sol * sol.price as syrup_usdc_sol_collat_usd,
        syrup_usdc_collat_btc,
        syrup_usdc_collat_btc * btc.price as syrup_usdc_btc_collat_usd,
        syrup_usdc_collat_eth,
        syrup_usdc_collat_eth * eth.price as syrup_usdc_eth_collat_usd, 
        syrup_usdc_collat_pt,
        syrup_usdc_collat_pt * 0.986 as syrup_usdc_pt_collat_usd,
        syrup_usdc_collat_orca,
        syrup_usdc_collat_orca * 1 as syrup_usdc_orca_collat_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }}, sol, btc, eth
),

syrup_usdc_totals AS (
    SELECT
        date,
        timestamp,
        pool_name,
        syrup_usdc_sol_collat_usd + syrup_usdc_btc_collat_usd + syrup_usdc_pt_collat_usd + syrup_usdc_orca_collat_usd + syrup_usdc_eth_collat_usd as collat_usd
    FROM syrup_usdc_collateral
),

-- Syrup USDT: Collateral
syrup_usdt_collateral AS (
    SELECT
        date,
        timestamp,
        'Syrup USDT' as pool_name,
        syrup_usdt_collat_sol,
        syrup_usdt_collat_sol * sol.price as syrup_usdt_sol_collat_usd,
        syrup_usdt_collat_btc,
        syrup_usdt_collat_btc * btc.price as syrup_usdt_btc_collat_usd
    FROM                
        {{ ref('fact_maple_otc_by_day') }}, sol, btc
),

syrup_usdt_totals AS (
    SELECT
        date,
        timestamp,
        pool_name,
        syrup_usdt_sol_collat_usd + syrup_usdt_btc_collat_usd as collat_usd
    FROM syrup_usdt_collateral
),

-- Altcoin Lending: Deposits, Loans, and Collateral
alt_lending AS (
    SELECT
        DATE(FROM_UNIXTIME(timestamp - 86399)) as date, 
        timestamp,
        alt_loans_sol,
        alt_loans_sol * sol.price as alt_sol_loans_usd,
        alt_deposits_sol,
        alt_deposits_sol * sol.price as alt_sol_deposits_usd,
        alt_loans_btc,
        alt_loans_btc * btc.price as alt_btc_loans_usd,
        alt_deposits_btc,
        alt_deposits_btc * btc.price as alt_btc_deposits_usd,
        alt_collat_btc,
        alt_collat_btc * btc.price as alt_btc_collat_usd,
        alt_loans_inj,
        alt_loans_inj * inj.price as alt_inj_loans_usd
    FROM
         {{ ref('fact_maple_otc_by_day') }}, sol, btc, inj
),

alt_totals AS (
    SELECT
        date,
        timestamp,
        'Altcoin Lending' as pool_name,
        alt_sol_loans_usd + alt_btc_loans_usd + alt_inj_loans_usd as outstanding,
        alt_sol_deposits_usd + alt_btc_deposits_usd + alt_btc_collat_usd + alt_inj_loans_usd as total_assets,
        alt_btc_collat_usd as collateral,
        alt_sol_deposits_usd + alt_btc_deposits_usd + alt_btc_collat_usd + alt_inj_loans_usd as tvl
    FROM
        alt_lending
    WHERE
        date IS NOT NULL AND date <= CURRENT_DATE
),

-- Solana Cash Management
solana AS (
    SELECT 
        DATE(FROM_UNIXTIME(date_unixtime)) as date, 
        date_unixtime as timestamp,
        'Maple Solana' as pool_name, 
        outstanding_usd as outstanding,
        outstanding_usd as total_assets,
        0 as collateral,
        outstanding_usd as tvl
    FROM 
        {{ ref('fact_maple_solana_by_day') }}
    WHERE 
        DATE(FROM_UNIXTIME(date_unixtime)) <= CURRENT_DATE
)

SELECT date, timestamp, pool_name, collat_usd as collateral, NULL as outstanding, NULL as total_assets, NULL as tvl
FROM bc_totals
UNION ALL
SELECT date, timestamp, pool_name, collat_usd as collateral, NULL as outstanding, NULL as total_assets, NULL as tvl
FROM hy_totals
UNION ALL
SELECT date, timestamp, pool_name, collat_usd as collateral, NULL as outstanding, NULL as total_assets, NULL as tvl
FROM syrup_usdc_totals
UNION ALL
SELECT date, timestamp, pool_name, collat_usd as collateral, NULL as outstanding, NULL as total_assets, NULL as tvl
FROM syrup_usdt_totals
UNION ALL
SELECT date, timestamp, pool_name, collateral, outstanding, total_assets, tvl
FROM alt_totals
UNION ALL
SELECT date, timestamp, pool_name, collateral, outstanding, total_assets, tvl
FROM solana