{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with raw as (
    SELECT
        block_timestamp,
        decoded_log:faceValue::number / 1e18 as eth_amount
    FROM
        fact_arbitrum_decoded_events
    WHERE
        contract_address = lower('0xa8bB618B1520E284046F3dFc448851A1Ff26e41B')
    AND event_name = 'WinningTicketRedeemed'
)
, prices as (
    {{ get_coingecko_price_with_latest('ethereum') }}
)
SELECT
    block_timestamp::date as date,
    sum(eth_amount) as fees_native,
    sum(eth_amount * p.price) as fees
FROM raw
LEFT JOIN prices p on p.date = raw.block_timestamp::date
GROUP BY 1
