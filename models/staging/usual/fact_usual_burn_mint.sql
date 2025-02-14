WITH usualBurned AS (
    SELECT 
        DATE(block_timestamp) AS date,
        SUM(TRY_CAST(raw_amount_precise AS numeric) / 1e18) AS daily_burned
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE 
        lower(contract_address) = lower('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND lower(to_address) = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
),
usualToTreasury AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(TRY_CAST(raw_amount_precise AS numeric) / 1e18) AS daily_treasury
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE
        lower(contract_address) = lower('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND lower(to_address) = lower('0x81ad394c0fa87e99ca46e1aca093bee020f203f4')
    GROUP BY 1
),
agg_data AS (
    SELECT
        t.date,
        COALESCE(t.daily_treasury, 0) AS daily_treasury,
        COALESCE(b.daily_burned, 0) AS daily_burned,
        SUM(COALESCE(t.daily_treasury, 0)) OVER (ORDER BY t.date) AS cumulative_treasury,
        SUM(COALESCE(b.daily_burned, 0)) OVER (ORDER BY t.date) AS cumulative_burned
    FROM usualToTreasury t
    LEFT JOIN usualBurned b USING (date)
),
net_balance AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        SUM(CASE 
            WHEN LOWER(decoded_log:"from") = '0x0000000000000000000000000000000000000000' 
            THEN decoded_log:"value" / 1e18
            WHEN LOWER(decoded_log:"to") = '0x0000000000000000000000000000000000000000' 
            THEN -decoded_log:"value" / 1e18
            ELSE 0
        END) AS tokens_mint
    FROM ethereum_flipside.core.fact_decoded_event_logs
    WHERE 
        LOWER(contract_address) = LOWER('0xC4441c2BE5d8fA8126822B9929CA0b81Ea0DE38E')
        AND event_name = 'Transfer'
    GROUP BY 1
),
final2 AS (
    SELECT 
        CAST(date AS DATE) AS date, 
        SUM(tokens_mint) OVER (ORDER BY date) AS cumulative_supply,
        tokens_mint AS daily_supply
    FROM net_balance
)
SELECT 
    f.date,
    f.daily_supply,
    f.cumulative_supply,
    COALESCE(a.daily_treasury, 0) AS daily_treasury,
    COALESCE(a.cumulative_treasury, 0) AS cumulative_treasury,
    COALESCE(a.daily_burned, 0) AS daily_burned,
    COALESCE(a.cumulative_burned, 0) AS cumulative_burned
FROM final2 f
LEFT JOIN agg_data a ON f.date = a.date
ORDER BY f.date DESC
