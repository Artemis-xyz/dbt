{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_polygon_ocr_reconcile_daily",
    )
}}
    

WITH
  reconcile_20231017_polygon_evt_transfer as (
    SELECT
      evt_transfer.from_address as admin_address,
      MAX(amount) as token_value
    FROM polygon_flipside.core.ez_token_transfers evt_transfer
    LEFT JOIN {{ ref('dim_chainlink_polygon_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = evt_transfer.from_address
    WHERE
      evt_transfer.block_timestamp >= '2023-10-16'
      AND evt_transfer.to_address = lower('0x2431d49d225C1BcCE7541deA6Da7aEf9C7AD3e23')
    GROUP BY
      EVT_TRANSFER.tx_hash,
      EVT_TRANSFER.event_index,
      evt_transfer.from_address
  ),
  reconcile_20231017_polygon_daily as (
    SELECT
      '2023-10-16' AS date_start,
      cast(date_trunc('month', cast('2023-10-16' as date)) as date) as date_month,
      admin_address,
      0 - SUM(token_value) as token_amount
    FROM
      reconcile_20231017_polygon_evt_transfer
    GROUP BY
      3
  ),
  reconcile_20231017_ethereum_evt_transfer as (
    SELECT
      evt_transfer.from_address as admin_address,
      MAX(amount) as token_value
    FROM polygon_flipside.core.ez_token_transfers evt_transfer
    LEFT JOIN {{ ref('dim_chainlink_polygon_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = evt_transfer.from_address
    WHERE
      evt_transfer.block_timestamp >= '2023-10-16'
      AND evt_transfer.from_address = lower('0xC489244f2a5FC0E65A0677560EAA4A13F5036ab6')
    GROUP BY
      EVT_TRANSFER.tx_hash,
      EVT_TRANSFER.event_index,
      evt_transfer.from_address
  ),
  reconcile_20231017_ethereum_daily as (
    SELECT
      '2023-10-16' AS date_start,
      cast(date_trunc('month', cast('2023-10-16' as date)) as date) as date_month,
      admin_address,
      0 - SUM(token_value) as token_amount
    FROM
      reconcile_20231017_ethereum_evt_transfer
    GROUP BY
      3
  )
SELECT
  COALESCE(reconcile_polygon.date_start, reconcile_ethereum.date_start) as date_start,
  COALESCE(reconcile_polygon.admin_address, reconcile_ethereum.admin_address) as admin_address,
  COALESCE(reconcile_polygon.token_amount, 0) + COALESCE(reconcile_ethereum.token_amount, 0) as token_amount
FROM 
  reconcile_20231017_polygon_daily reconcile_polygon
FULL OUTER JOIN reconcile_20231017_ethereum_daily reconcile_ethereum ON reconcile_ethereum.admin_address = reconcile_polygon.admin_address