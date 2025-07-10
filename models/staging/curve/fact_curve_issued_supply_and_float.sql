{{
    config(
        materialized="table"
    )
}}


WITH minted AS (
    SELECT 
        block_timestamp::date AS date,
        SUM(amount) AS daily_mint,
        SUM(SUM(amount)) OVER (ORDER BY block_timestamp::date) AS total_supply
    FROM {{ source("ETHEREUM_FLIPSIDE", "ez_token_transfers") }}
    WHERE 
        lower(contract_address) = '0xd533a949740bb3306d119cc777fa900ba034cd52'
        AND lower(from_address) = '0x0000000000000000000000000000000000000000'
    GROUP BY block_timestamp::date
),

-- FOUNDATION WALLET BALANCE
foundation_balance_raw AS (
    SELECT 
        block_timestamp::date AS date,
        MAX(balance / 1e18) AS foundation_balance
    FROM {{ source("ETHEREUM_FLIPSIDE", "fact_token_balances") }}
    WHERE 
        lower(user_address) = '0xe3997288987e6297ad550a69b31439504f513267'
        AND lower(contract_address) = '0xd533a949740bb3306d119cc777fa900ba034cd52'
    GROUP BY block_timestamp::date
),

-- FILL FOUNDATION BALANCE FOR MISSING DAYS
supply_with_foundation AS (
    SELECT 
        m.date,
        m.total_supply,
        f.foundation_balance,
        LAST_VALUE(f.foundation_balance IGNORE NULLS) OVER (
            ORDER BY m.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_foundation_balance
    FROM minted m
    LEFT JOIN foundation_balance_raw f ON m.date = f.date
),

-- UNVESTED BALANCE = NET HOLDINGS OF TEAM WALLETS
inflows AS (
    SELECT 
        block_timestamp::date AS date,
        SUM(amount) AS inflow
    FROM {{ source("ETHEREUM_FLIPSIDE", "ez_token_transfers") }}
    WHERE 
        lower(to_address) IN (
            '0xd2d43555134dc575bf7279f4ba18809645db0f1d',
            '0x575ccd8e2d300e2377b43478339e364000318e2c',
            '0xf22995a3ea2c83f6764c711115b23a88411cafdd',
            '0x2a7d59e327759acd5d11a8fb652bf4072d28ac04',
            '0xf7dbc322d72c1788a1e37eee738e2ea9c7fa875e',
            '0x679fcb9b33fc4ae10ff4f96caef49c1ae3f8fa67',
            '0x41df5d28c7e801c4df0ab33421e2ed6ce52d2567'
        )
        AND symbol = 'CRV'
    GROUP BY date
),
outflows AS (
    SELECT 
        block_timestamp::date AS date,
        SUM(amount) AS outflow
    FROM {{ source("ETHEREUM_FLIPSIDE", "ez_token_transfers") }}
    WHERE 
        lower(from_address) IN (
            '0xd2d43555134dc575bf7279f4ba18809645db0f1d',
            '0x575ccd8e2d300e2377b43478339e364000318e2c',
            '0xf22995a3ea2c83f6764c711115b23a88411cafdd',
            '0x2a7d59e327759acd5d11a8fb652bf4072d28ac04',
            '0xf7dbc322d72c1788a1e37eee738e2ea9c7fa875e',
            '0x679fcb9b33fc4ae10ff4f96caef49c1ae3f8fa67',
            '0x41df5d28c7e801c4df0ab33421e2ed6ce52d2567'
        )
        AND symbol = 'CRV'
    GROUP BY date
),
net_changes AS (
    SELECT 
        COALESCE(i.date, o.date) AS date,
        COALESCE(i.inflow, 0) AS inflow,
        COALESCE(o.outflow, 0) AS outflow,
        COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0) AS net_change
    FROM inflows i
    FULL OUTER JOIN outflows o ON i.date = o.date
),
unvested_balance AS (
    SELECT 
        date,
        SUM(net_change) OVER (ORDER BY date) AS unvested
    FROM net_changes
),

-- LOCKED SUPPLY FROM veCRV CONTRACT
locked_balance_raw AS (
    SELECT 
        block_timestamp::date AS date,
        MAX(balance / 1e18) AS locked
    FROM {{ source("ETHEREUM_FLIPSIDE", "fact_token_balances") }}
    WHERE 
        lower(user_address) = '0x5f3b5dfeb7b28cdbd7faba78963ee202a494e2a2'
        AND lower(contract_address) = '0xd533a949740bb3306d119cc777fa900ba034cd52'
    GROUP BY block_timestamp::date
),

-- COMBINE EVERYTHING
final AS (
    SELECT 
        s.date,
        s.total_supply,
        s.filled_foundation_balance,
        u.unvested,
        LAST_VALUE(u.unvested IGNORE NULLS) OVER (
            ORDER BY s.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_unvested,
        l.locked,
        LAST_VALUE(l.locked IGNORE NULLS) OVER (
            ORDER BY s.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_locked
    FROM supply_with_foundation s
    LEFT JOIN unvested_balance u ON s.date = u.date
    LEFT JOIN locked_balance_raw l ON s.date = l.date
)

-- FINAL OUTPUT
SELECT 
    date,
    3030303031 as max_supply,
    3030303031 - total_supply as uncreated_tokens,
    total_supply,
    0 as burned_crv,
    COALESCE(filled_foundation_balance, 0) AS foundation_balance,
    COALESCE(filled_unvested, 0) AS unvested,
    COALESCE(filled_locked, 0) AS locked,
    total_supply - filled_foundation_balance AS issued_supply,
    total_supply - filled_foundation_balance - filled_unvested - filled_locked AS circulating_supply
FROM final
ORDER BY date