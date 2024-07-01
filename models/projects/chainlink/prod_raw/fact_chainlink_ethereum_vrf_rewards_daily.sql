{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_rewards_daily",
    )
}}

WITH
  link_usd_daily AS (
    {{get_coingecko_price_with_latest("chainlink")}}
  ),
  vrf_reward_daily AS (
    SELECT
      vrf_daily.date_start,
      COALESCE(vrf_daily.token_amount, 0) as token_amount,
      COALESCE(vrf_daily.token_amount * lud.price, 0)  as usd_amount
    FROM
      {{ref('fact_chainlink_ethereum_vrf_request_fulfilled_daily')}} vrf_daily
    LEFT JOIN link_usd_daily lud ON lud.date = vrf_daily.date_start
    ORDER BY date_start
  )
SELECT
  'ethereum' as blockchain,
  date_start as date,
  token_amount,
  usd_amount
FROM
  vrf_reward_daily
ORDER BY
  2