{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN'
    )
}}

--This is because they had a faulty contract on ethereum on 9/13/2023
WITH date_range AS (
    SELECT DISTINCT
        date::date as date
    FROM (
        SELECT 
            dateadd(
                'day',
                seq4(),
                CAST('2023-09-14' as date)
            ) as date
        FROM TABLE(GENERATOR(ROWCOUNT => 500)) 
    )
    WHERE date <= CURRENT_DATE()
),

transfers AS (
    SELECT
        date_trunc('day', BLOCK_TIMESTAMP)::date as date,
        FROM_ADDRESS as sender,
        TO_ADDRESS as recipient,
        RAW_AMOUNT / 1e18 as amount,
        CASE 
            WHEN lower(TO_ADDRESS) = lower('0x000000000000000000000000000000000000dead') 
            AND NOT (
                lower(FROM_ADDRESS) = lower('0xDa74C6B4E6813bdb83cb4cff6ad4eB8D43F34B0D')
                AND (RAW_AMOUNT / 1e18) >= 50000  -- Exclude treasury-unlock-burns >= 50000
            ) THEN 'burn'
            WHEN lower(FROM_ADDRESS) = lower('0xECC6c8C7EdA9C600773F0D133549d9933a91dBFB') THEN 'reward'
            ELSE 'other'
        END as transfer_type
    FROM ETHEREUM_FLIPSIDE.CORE.FACT_TOKEN_TRANSFERS
    WHERE lower(CONTRACT_ADDRESS) = lower('0x38E68A37E401F7271568CecaAc63c6B1e19130B4') -- BANANA V2
        AND BLOCK_NUMBER >= 18135851
),

daily_burns AS (
    SELECT
        date,
        SUM(amount) * -1 as burn_amount
    FROM transfers
    WHERE transfer_type = 'burn'
    GROUP BY date
),

daily_rewards AS (
    SELECT
        date,
        SUM(amount) as reward_amount
    FROM transfers
    WHERE transfer_type = 'reward'
    GROUP BY date
),

airdrops AS (
    SELECT
        CAST('2023-10-24 00:00:00.000' as date) as date,
        120000 as airdrop_amount
),

daily_metrics AS (
    SELECT
        d.date,
        COALESCE(r.reward_amount, 0) as rewards,
        COALESCE(b.burn_amount, 0) as burns,
        COALESCE(a.airdrop_amount, 0) as airdrops,
        COALESCE(r.reward_amount, 0) + COALESCE(b.burn_amount, 0) + COALESCE(a.airdrop_amount, 0) as net_supply_change
    FROM date_range d
    LEFT JOIN daily_burns b ON d.date = b.date
    LEFT JOIN daily_rewards r ON d.date = r.date
    LEFT JOIN airdrops a ON d.date = a.date
)

SELECT
    date,
    net_supply_change,
    2500000 + SUM(net_supply_change) OVER (ORDER BY date) as circulating_supply,
    burns,
    rewards as gross_emissions,
    airdrops as pre_mine_unlocks
FROM daily_metrics
WHERE date IS NOT NULL
ORDER BY date DESC 