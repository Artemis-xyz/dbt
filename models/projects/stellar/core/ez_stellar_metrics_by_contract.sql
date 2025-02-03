{{
    config(
        materialized="table",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}
WITH fundamental_data AS (
    SELECT
        * EXCLUDE date,
        TO_TIMESTAMP_NTZ(date) AS date
    FROM {{ source('PROD_LANDING', 'ez_stellar_metrics_by_contract') }}
), prices as ({{ get_coingecko_price_with_latest("stellar") }})
SELECT
    fundamental_data.date,
    fundamental_data.chain,
    fundamental_data.app,
    fundamental_data.category,
    fundamental_data.name,
    fundamental_data.friendly_name,
    fundamental_data.contract_address,
    fundamental_data.classic_txns AS txns,
    fundamental_data.soroban_txns AS soroban_txns,
    fundamental_data.daily_fees as gas,
    fundamental_data.daily_fees * price as gas_usd,
    fundamental_data.operations as operations,
    fundamental_data.dau as dau
FROM fundamental_data
LEFT JOIN prices USING(date)