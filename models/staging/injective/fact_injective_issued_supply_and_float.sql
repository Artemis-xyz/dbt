{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
    )
}}


WITH mints AS (
    SELECT 
        Date,
        Mints
    FROM {{ ref("fact_injective_mints_silver") }}
),
outflows AS (
    SELECT 
        Date, 
        OUTFLOWS
    FROM {{ ref("fact_injective_unlocks") }}
),
revenue AS (
    SELECT 
        Date,
        revenue_native
    FROM {{ ref("fact_injective_revenue_silver") }}
),
combined AS (
    SELECT 
        COALESCE(m.date, o.date, r.date) AS date,
        COALESCE(m.mints, 0) AS mints,
        COALESCE(o.outflows, 0) AS outflows,
        COALESCE(r.revenue_native, 0) AS revenue_native,
        COALESCE(r.revenue_native, 0) AS burned_inj
    FROM mints m
    FULL OUTER JOIN outflows o ON m.date = o.date
    FULL OUTER JOIN revenue r ON COALESCE(m.date, o.date) = r.date
),
cumulative_supply AS (
    SELECT 
        date,
        SUM(mints + outflows - revenue_native) OVER (ORDER BY date) AS circulating_supply,
        revenue_native as burned_inj
    FROM combined
),
latest_rewards_json AS (
    SELECT 
        PARSE_JSON(SOURCE_JSON) AS parsed_json
    FROM {{ source('PROD_LANDING', 'raw_injective_mints') }} 
    QUALIFY ROW_NUMBER() OVER (ORDER BY EXTRACTION_DATE DESC) = 1
),
rewards_data AS (
    SELECT 
        TO_DATE(TO_TIMESTAMP_LTZ(f.value:"date"::NUMBER / 1000)) AS date,
        f.value:"staker_rewards"::FLOAT AS staker_rewards
    FROM latest_rewards_json,
         LATERAL FLATTEN(input => parsed_json) f
),
cumulative_rewards AS (
    SELECT 
        date,
        SUM(staker_rewards) OVER (ORDER BY date) AS cumulative_rewards
    FROM rewards_data
),
foundation_balance_raw AS (
    SELECT 
        block_timestamp::date AS date,
        MAX(balance / 1e18) AS foundation_balance
    FROM {{ source('ETHEREUM_FLIPSIDE', 'fact_token_balances') }} 
    WHERE 
        lower(user_address) = lower('0x7E233EAfC76243474369bd080238fD6EB36A73CE')
        AND lower(contract_address) = lower('0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30')
    GROUP BY block_timestamp::date
),
date_spine AS (
    SELECT DISTINCT date FROM cumulative_supply
),
foundation_balance_filled AS (
    SELECT 
        d.date,
        LAST_VALUE(f.foundation_balance IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS foundation_wallet_balance
    FROM date_spine d
    LEFT JOIN foundation_balance_raw f ON d.date = f.date
),
vesting_data AS (
    SELECT 
        date,
        SUM(outflows) AS outflows,
        SUM(SUM(outflows)) OVER (ORDER BY date) AS cumulative_unlocks
    FROM {{ ref("fact_injective_unlocks") }}
    GROUP BY date
)

SELECT 
    cs.date,
    cs.circulating_supply,
    cs.burned_inj,
    COALESCE(cr.cumulative_rewards, 0) AS cumulative_rewards,
    COALESCE(fbf.foundation_wallet_balance, 0) AS foundation_wallet_balance,
    100000000 + COALESCE(cr.cumulative_rewards, 0) - COALESCE(fbf.foundation_wallet_balance, 0)
    - cs.burned_inj AS issued_supply,
    COALESCE(v.cumulative_unlocks, 0) AS cumulative_unlocks,
    100000000 + COALESCE(cr.cumulative_rewards, 0) AS total_supply
FROM cumulative_supply cs
LEFT JOIN cumulative_rewards cr ON cs.date = cr.date
LEFT JOIN foundation_balance_filled fbf ON cs.date = fbf.date
LEFT JOIN vesting_data v ON cs.date = v.date
ORDER BY cs.date
