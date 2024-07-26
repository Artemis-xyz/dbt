{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_chart_of_accounts"
    )
}}

SELECT code, primary_label, secondary_label, account_label, category_label, subcategory_label
    FROM (VALUES
    (11110, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'ETH', 'ETH'),
    (11120, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'BTC', 'BTC'),
    (11130, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'WSTETH', 'WSTETH'),
    (11140, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Stable LP'),
    (11141, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Volatile LP'),
    (11199, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Other', 'Other'),
    (11210, 'Assets', 'Collateralized Lending', 'Money Market', 'Money Market', 'D3M'),
    (11510, 'Assets', 'Collateralized Lending', 'Legacy', 'Stablecoins', 'Stablecoins'),
    (12310, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Off-Chain Private Credit'),
    (12311, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Tokenized Private Credit'),
    (12320, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Off-Chain Public Credit'),
    (12321, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Tokenized Public Credit'),
    (13410, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Non-Yielding Stablecoin'),
    (13411, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Yielding Stablecoin'),
    (14620, 'Assets', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (19999, 'Assets', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),

    (21110, 'Liabilities', 'Stablecoin', 'Circulating', 'Interest-bearing', 'Dai'),
    (21120, 'Liabilities', 'Stablecoin', 'Circulating', 'Non-interest bearing', 'Dai'),
    (29999, 'Liabilities', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),

    (31110, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'ETH', 'ETH SF'),
    (31120, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'BTC', 'BTC SF'),
    (31130, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'WSTETH', 'WSTETH SF'),
    (31140, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Stable LP SF'),
    (31141, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Volatile LP SF'),
    (31150, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Other', 'Other SF'),
    (31160, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Money Market', 'D3M SF'),
    (31170, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Private Credit SF'),
    (31171, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Private Credit SF'),
    (31172, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Public Credit Interest'),
    (31173, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Public Credit Interest'),
    (31180, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'PSM', 'Yielding Stablecoin Interest'),
    (31190, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Stablecoins', 'Stablecoins SF'),
    (31210, 'Equity', 'Protocol Surplus', 'Liquidation Revenues', 'Liquidation Revenues', 'Liquidation Revenues'),
    (31310, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Trading Revenues'),
    --(31311, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Teleport Revenues'),  --needs to be added still
    (31410, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Mints', 'MKR Mints'),
    (31420, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Burns', 'MKR Burns'),
    (31510, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Inflow', 'Sin Inflow'),
    (31520, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Outflow', 'Sin Outflow'),
    (31610, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'DSR', 'Circulating Dai'),
    (31620, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Liquidation Expenses', 'Liquidation Expenses'),
    (31630, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Oracle Gas Expenses', 'Oracle Gas Expenses'),
    (31710, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Keeper Maintenance', 'Keeper Maintenance'),
    (31720, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Workforce Expenses'),
    (31730, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Returned Workforce Expenses'),
    (31740, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Direct to Third Party Expenses', 'Direct to Third Party Expenses'),
    (31810, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Direct MKR Token Expenses', 'Direct MKR Token Expenses'),
    (32810, 'Equity', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
    (33110, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Vested MKR Token Expenses', 'Vested MKR Token Expenses'),
    (34110, 'Equity', 'Reserved MKR Surplus', 'MKR Contra Equity', 'MKR Contra Equity', 'MKR Contra Equity'),
    (39999, 'Equity', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token')
    ) AS t(code, primary_label, secondary_label, account_label, category_label, subcategory_label)