{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_fm_reward_evt_transfer_daily"
    )
}}

SELECT
    'ethereum' as chain
    , evt_block_time::date AS date_start
    , MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month
    , fm_reward_evt_transfer.admin_address as admin_address
    , MAX(fm_reward_evt_transfer.operator_name) as operator_name
    , SUM(token_value) as token_amount
FROM {{ref('fact_chainlink_ethereum_fm_reward_evt_transfer')}} fm_reward_evt_transfer
LEFT JOIN {{ ref('dim_chainlink_ethereum_ocr_operator_admin_meta') }} fm_operator_admin_meta ON lower(fm_operator_admin_meta.admin_address) = lower(fm_reward_evt_transfer.admin_address)
GROUP BY
  2, 4
ORDER BY
  2, 4