{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

WITH fees AS (
  SELECT
    TO_DATE(SUBSTR(value:"date"::STRING, 1, 10), 'YYYY-MM-DD')    AS date,
    value:"quote_mint"::STRING                                    AS quote_mint,
    value:"total_protocol_fees"::NUMBER                           AS total_protocol_fees,
    value:"total_referral_fees"::NUMBER                           AS total_referral_fees,
    value:"total_trading_fees"::NUMBER                            AS total_trading_fees,
    value:"total_volume"::NUMBER                                  AS total_volume
  FROM landing_database.prod_landing.raw_jupiter_studio_fees_revs,
       LATERAL FLATTEN(input => source_json)
),
daily_price AS (
  SELECT
    DATE_TRUNC('day', hour)              AS date,
    token_address                        AS quote_mint,
    AVG(price)                           AS avg_usd_price,
    MAX(decimals)                        AS decimals
  FROM {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly')}}
  GROUP BY 1, 2
),
per_token_usd AS (
  SELECT
    f.date,
    f.quote_mint,
    -- convert from smallest unit then multiply by USD price
    (f.total_protocol_fees  / POWER(10, dp.decimals)) * dp.avg_usd_price  AS protocol_fees_usd,
    (f.total_referral_fees  / POWER(10, dp.decimals)) * dp.avg_usd_price  AS referral_fees_usd,
    (f.total_trading_fees   / POWER(10, dp.decimals)) * dp.avg_usd_price  AS trading_fees_usd,
    (f.total_volume         / POWER(10, dp.decimals)) * dp.avg_usd_price  AS volume_usd
  FROM fees f
  LEFT JOIN daily_price dp
    ON f.date = dp.date
   AND f.quote_mint = dp.quote_mint
)
SELECT
  date,
  SUM(protocol_fees_usd)  AS total_protocol_fees_usd,
  SUM(referral_fees_usd)  AS total_referral_fees_usd,
  SUM(trading_fees_usd)   AS total_jup_studio_revenue_usd,
  SUM(protocol_fees_usd) + SUM(referral_fees_usd) + SUM(trading_fees_usd) AS total_fees,
  SUM(volume_usd)         AS total_volume_usd
FROM per_token_usd
GROUP BY date
ORDER BY date
