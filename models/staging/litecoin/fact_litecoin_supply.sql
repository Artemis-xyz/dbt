WITH block_rewards AS (
  SELECT 
    distinct
    number,
    timestamp,
    DATE_TRUNC('day', timestamp) AS date,
    POW(2, FLOOR(number/840000)) as div,
    50 / div AS block_reward
  FROM {{ ref('fact_litecoin_blocks') }}
)

,daily_supply AS (
  -- Aggregate by day
  SELECT 
    date,
    count(*) as count_blocks,
    avg(block_reward) as block_reward_avg,
    MAX(number) AS last_block_of_day,
    SUM(block_reward) AS daily_new_supply,
    SUM(SUM(block_reward)) OVER (ORDER BY date) AS cumulative_supply
  FROM block_rewards
  GROUP BY date
)
SELECT 
    date,
    84000000 as max_supply,
    84000000 - cumulative_supply as uncreated_tokens,
    cumulative_supply as total_supply,
    cumulative_supply as issued_supply,
    cumulative_supply as circulating_supply,
    last_block_of_day,
    daily_new_supply,
    cumulative_supply,
    ROUND(cumulative_supply / 84000000 * 100, 4) AS supply_percentage,
    -- Annual inflation rate (approximate)
    CASE 
    WHEN LAG(cumulative_supply, 365) OVER (ORDER BY date) > 0 
    THEN ROUND(((cumulative_supply - LAG(cumulative_supply, 365) OVER (ORDER BY date)) 
            / LAG(cumulative_supply, 365) OVER (ORDER BY date)) * 100, 2)
    ELSE NULL 
    END AS annual_inflation_rate,
    block_reward_avg
FROM daily_supply
ORDER BY date