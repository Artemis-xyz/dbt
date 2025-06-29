{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}


with raw as (
SELECT
    block_timestamp,
    decoded_log:_amount::number / 1e18 as amount_native
FROM
    {{ source("POLYGON_FLIPSIDE", "ez_decoded_event_logs") }}
WHERE TRUE
AND contract_address = lower('0x8129f3cd3eba82136caf5ab87e2321c958da5b63')
AND event_name = 'TokensTransferred'
)
, prices as (
    {{ get_coingecko_price_with_latest('dimo') }}
)
SELECT
    block_timestamp::date as date,
    sum(amount_native) as token_incentives_native,
    sum(amount_native * p.price) as token_incentives
FROM raw
LEFT JOIN prices p on p.date = raw.block_timestamp::date
GROUP BY 1