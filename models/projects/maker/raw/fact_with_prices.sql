{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_with_prices"
    )
}}

WITH unioned_data AS (
    SELECT ts, hash, code, value, 'DAI' AS token, 'Returned Workforce Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_team_dai_burns') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Liquidation Revenues/Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_liquidation') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Trading Revenues' AS descriptor, ilk FROM {{ ref('fact_trading_revenues') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'MKR Mints' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_mkr_mints') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'MKR Burns' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_mkr_burns') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Interest Accruals' AS descriptor, ilk FROM {{ ref('fact_interest_accruals') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'OpEx' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_opex') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'DSR Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_dsr_expenses') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Other Sin Outflows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_other_sin_outflows') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Sin Inflows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_sin_inflows') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'DSR Flows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_dsr_flows') }}
    UNION ALL
    SELECT ts, hash, code, value, token, 'Treasury Flows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_treasury_flows') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Loan Draws/Repays' AS descriptor, ilk FROM {{ ref('fact_loan_actions') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'D3M Revenues' AS descriptor, ilk FROM {{ ref('fact_d3m_revenues') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'PSM Yield' AS descriptor, ilk FROM {{ ref('fact_psm_yield') }}
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'RWA Yield' AS descriptor, ilk FROM {{ ref('fact_rwa_yield') }}
    UNION ALL
    SELECT ts, hash, code, value, 'MKR' AS token, 'MKR Vest Creates/Yanks' AS descriptor, NULL AS ilk FROM {{ ref('fact_mkr_vest_creates_yanks') }}
    UNION ALL
    SELECT ts, hash, code, value, 'MKR' AS token, 'MKR Pause Proxy Trxns' AS descriptor, NULL AS ilk FROM {{ ref('fact_pause_proxy_mkr_trxns') }}
    UNION ALL
    SELECT ts, NULL AS hash, 19999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_m2m_levels') }}
    UNION ALL
    SELECT ts, NULL AS hash, 29999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_m2m_levels') }}
    UNION ALL
    SELECT ts, NULL AS hash, 39999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM {{ ref('fact_m2m_levels') }}
)

SELECT 
    coa.code,
    u.ts,
    u.hash,
    u.value,
    u.token,
    u.descriptor,
    u.ilk,
    u.value * CASE WHEN u.token = 'DAI' THEN 1 ELSE tp.price END AS dai_value,
    u.value * CASE WHEN u.token = 'DAI' THEN 1 ELSE tp.price END / ep.price AS eth_value,
    ep.price AS eth_price
FROM {{ ref('dim_chart_of_accounts') }} coa
LEFT JOIN unioned_data u USING (code)
LEFT JOIN {{ ref('fact_token_prices') }} tp 
    ON DATE_TRUNC('day', u.ts) = DATE_TRUNC('day', tp.ts)
    AND EXTRACT(HOUR FROM u.ts) = EXTRACT(HOUR FROM tp.ts)
    AND u.token = tp.token
LEFT JOIN {{ ref('fact_eth_prices') }} ep
    ON DATE_TRUNC('day', u.ts) = DATE_TRUNC('day', ep.ts)
    AND EXTRACT(HOUR FROM u.ts) = EXTRACT(HOUR FROM ep.ts)
WHERE u.value IS NOT NULL