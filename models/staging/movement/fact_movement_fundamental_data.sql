{{
    config(
        materialized="incremental",
        snowflake_warehouse="MOVEMENT",
    )
}}

SELECT
    block_timestamp::date as date
    , count(distinct tx_hash) as txns
    , count(distinct sender) as dau
    , sum(gas_used * gas_unit_price / 1e8) as gas_native
    , sum(gas_used * gas_unit_price / 1e8 * p.price) as gas
FROM {{ source("MOVEMENT_FLIPSIDE", "fact_transactions") }}
LEFT JOIN {{ source("MOVEMENT_FLIPSIDE_PRICE", "ez_prices_hourly") }} p ON is_native AND p.hour = date_trunc('hour', block_timestamp)
{% if is_incremental() %}
    WHERE block_timestamp >= (SELECT DATE_ADD('day', -3, MAX(date)) FROM {{ this }})
{% endif %}
GROUP BY 1