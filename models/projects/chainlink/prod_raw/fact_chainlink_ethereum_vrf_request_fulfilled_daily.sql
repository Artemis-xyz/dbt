{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_request_fulfilled_daily",
    )
}}


SELECT
  'ethereum' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  SUM(token_value) as token_amount
FROM
  {{ref('fact_chainlink_ethereum_vrf_request_fulfilled')}} vrf_request_fulfilled
GROUP BY
  2
ORDER BY
  2