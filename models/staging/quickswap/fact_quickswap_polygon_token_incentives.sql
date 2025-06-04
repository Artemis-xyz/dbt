{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL"

    )
}}

WITH farming_rewards AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    block_timestamp,
    TRY_CAST(decoded_log:"reward"::string AS NUMBER) AS reward,
    TRY_CAST(decoded_log:"bonusReward"::string AS NUMBER) AS bonus_reward,
    decoded_log:"rewardAddress" AS reward_token
  FROM polygon_flipside.core.ez_decoded_event_logs
  WHERE lower(contract_address) IN (
    lower('0x8a26436e41d0b5fc4c6ed36c1976fafbe173444e'),
    lower('0x9923f42a02A82dA63EE0DbbC5f8E311e3DD8A1f8')
  )
    AND event_name IN ('RewardClaimed', 'FarmEnded')
    AND lower(decoded_log:"rewardAddress") IN (
        lower('0x958d208cdf087843e9ad98d23823d32e17d723a1'),
        lower('0xf28164A485B0B2C90639E47b0f377b4a438a16B1'),
        lower('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    )
),

combined_rewards AS (
  SELECT
    day,
    block_timestamp,
    COALESCE(reward, 0) + COALESCE(bonus_reward, 0) AS total_reward
  FROM farming_rewards
),

usd_rewards_raw AS (
  SELECT
    cr.day,
    cr.total_reward / 1e18 AS dquick_tokens,
    ep.price AS raw_price_usd,
    (cr.total_reward / 1e18) * ep.price AS reward_usd
  FROM combined_rewards cr
  LEFT JOIN polygon_flipside.price.ez_prices_hourly ep
    ON lower(ep.token_address) = lower('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND ep.hour = DATE_TRUNC('hour', cr.block_timestamp)
),

burn_data AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    SUM(AMOUNT_USD) AS daily_usd_burned
  FROM polygon_flipside.core.ez_token_transfers
  WHERE LOWER(CONTRACT_ADDRESS) = LOWER('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND LOWER(TO_ADDRESS) = LOWER('0x000000000000000000000000000000000000dead')
  GROUP BY 1
),

buyback_data AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    SUM(AMOUNT_USD) AS daily_usd_buyback
  FROM polygon_flipside.core.ez_token_transfers
  WHERE LOWER(CONTRACT_ADDRESS) = LOWER('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND LOWER(FROM_ADDRESS) IN (
        LOWER('0xbd098dB9AD3dbaF2bDAF581340B2662d9A3CA8D2'),
        LOWER('0x584255c0ee2b9Dd653ed4C9ED95DB64bA500E842')
    )
    AND LOWER(TO_ADDRESS) IN (
        LOWER('0x958d208cdf087843e9ad98d23823d32e17d723a1'),
        LOWER('0xB5C064F955D8e7F38fE0460C556a72987494eE17')
    )
  GROUP BY 1
),

daily_rewards AS (
  SELECT
    u.day,
    ROUND(COALESCE(SUM(u.dquick_tokens), 0), 4) AS total_dquick_tokens,
    AVG(u.raw_price_usd) AS avg_price_raw,
    ROUND(COALESCE(SUM(u.reward_usd), 0), 2) AS total_usd_reward
  FROM usd_rewards_raw u
  GROUP BY u.day
),

forward_filled_prices AS (
  SELECT
    day,
    total_dquick_tokens,
    LAST_VALUE(avg_price_raw IGNORE NULLS) OVER (
      ORDER BY day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS avg_price_usd,
    total_usd_reward
  FROM daily_rewards
),

burn_buyback_combined AS (
  SELECT
    COALESCE(bu.day, bb.day) AS day,
    COALESCE(bu.daily_usd_burned, 0) AS daily_usd_burned,
    COALESCE(bb.daily_usd_buyback, 0) AS daily_usd_buyback
  FROM burn_data bu
  FULL OUTER JOIN buyback_data bb ON bu.day = bb.day
),

quickswap_staking_rewards AS (
  SELECT
    DATE(tt.block_timestamp) AS date,
    SUM(tt.decoded_log:"amount"::FLOAT / 1e18) AS rewards,
    SUM((tt.decoded_log:"amount"::FLOAT / 1e18) * eph.price) AS token_incentives
  FROM polygon_flipside.core.ez_decoded_event_logs AS tt
  JOIN polygon_flipside.price.ez_prices_hourly AS eph
    ON LOWER(eph.token_address) = LOWER('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
  WHERE LOWER(tt.contract_address) IN (
    LOWER('0x158B99aE660D4511e4c52799e1c47613cA47a78a'),
    LOWER('0xc35f556C8Ac05FB484A703eE96A2f997F8CAC957'),
    LOWER('0xFa3deAFecd0Fad0b083AB050cF30E1d541720680'),
    LOWER('0xC84Ec966b4E6473249d64763366212D175b5c2bd'),
    LOWER('0x9bA5cE366f99f2C10A38ED35159c60CC558ca626'),
    LOWER('0xB6985ce301E6C6c4766b4479dDdc152d4eD0f2d3'),
    LOWER('0xAE361aB6c12E9C8aFf711D3Ddc178be6dA2A7472'),
    LOWER('0x5554EdCaf47189894315d845D5B19eeB14D79048'),
    LOWER('0x780E2496141F97Dd48ed8cb296f5C0828f1CB317'),
    LOWER('0x7636e51D352cf89a7A05aE7b66f8841c368080Ff'),
    LOWER('0x88F54579fbB2a33c33342709A4B2a9A07dA94EE2')
  )
    AND tt.event_name = 'LogOnReward'
  GROUP BY date
),

quick_transfers AS (
  SELECT
    DATE(tt.block_timestamp) AS date,
    SUM((tt.decoded_log:"amount"::FLOAT / 1e18) * eph.price) AS quick_amount_usd
  FROM polygon_flipside.core.ez_decoded_event_logs tt
  JOIN polygon_flipside.price.ez_prices_hourly eph
    ON eph.token_address = LOWER('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
  WHERE LOWER(tt.contract_address) = LOWER('0xb5c064f955d8e7f38fe0460c556a72987494ee17')
    AND tt.event_name = 'Transfer'
    AND LOWER(tt.decoded_log:"from") = LOWER('0x3a381497813208508689d78c90ec9fb115d5640d')
  GROUP BY date
),

quickswap_combined AS (
  SELECT
    qsr.date,
    qsr.rewards,
    qsr.token_incentives + COALESCE(qt.quick_amount_usd, 0) AS total_quickswap_token_incentives
  FROM quickswap_staking_rewards qsr
  LEFT JOIN quick_transfers qt ON qsr.date = qt.date
),

all_dates AS (
  SELECT day FROM forward_filled_prices
  UNION
  SELECT day FROM burn_buyback_combined  
  UNION
  SELECT date AS day FROM quickswap_combined
)

SELECT
  ad.day,
  COALESCE(f.total_dquick_tokens, 0) AS total_dquick_tokens,
  ROUND(f.avg_price_usd, 6) AS avg_price_usd,
  COALESCE(f.total_usd_reward, 0) AS farming_usd_reward,
  ROUND(SUM(COALESCE(f.total_usd_reward, 0)) OVER (ORDER BY ad.day), 2) AS cumulative_farming_reward,
  COALESCE(b.daily_usd_burned, 0) AS daily_usd_burned,
  SUM(COALESCE(b.daily_usd_burned, 0)) OVER (ORDER BY ad.day) AS cumulative_usd_burned,
  COALESCE(b.daily_usd_buyback, 0) AS daily_usd_buyback,
  SUM(COALESCE(b.daily_usd_buyback, 0)) OVER (ORDER BY ad.day) AS cumulative_usd_buyback,
  COALESCE(qc.rewards, 0) AS quickswap_rewards_tokens,
  COALESCE(qc.total_quickswap_token_incentives, 0) AS quickswap_staking_usd,
  COALESCE(f.total_usd_reward, 0) + COALESCE(b.daily_usd_burned, 0) + COALESCE(b.daily_usd_buyback, 0) + COALESCE(qc.total_quickswap_token_incentives, 0) AS total_daily_token_incentive,
  SUM(COALESCE(f.total_usd_reward, 0) + COALESCE(b.daily_usd_burned, 0) + COALESCE(b.daily_usd_buyback, 0) + COALESCE(qc.total_quickswap_token_incentives, 0))
      OVER (ORDER BY ad.day) AS cumulative_token_incentive
FROM all_dates ad
LEFT JOIN forward_filled_prices f ON ad.day = f.day
LEFT JOIN burn_buyback_combined b ON ad.day = b.day
LEFT JOIN quickswap_combined qc ON ad.day = qc.date
ORDER BY ad.day