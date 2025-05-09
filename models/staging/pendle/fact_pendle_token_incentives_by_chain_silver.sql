{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}

with agg as(
    select
        date(block_timestamp) as date
        , 'ethereum' as chain
        , sum(raw_amount_precise::number / 1e18) as amt_pendle
        , SUM(raw_amount_precise::number / 1e18 * p.price) as amt_usd
    from
        ethereum_flipside.core.fact_token_transfers
        left join ethereum_flipside.price.ez_prices_hourly p ON p.hour = date_trunc('hour', block_timestamp)
        AND p.token_address = lower('0x808507121B80c02388fAd14726482e061B8da827')
    where
        from_address = lower('0x47D74516B33eD5D70ddE7119A40839f6Fcc24e57') and -- GaugeController
        contract_address = lower('0x808507121B80c02388fAd14726482e061B8da827')
    GROUP BY 1
    UNION ALL
    select
        date(block_timestamp) as date
        , 'arbitrum' as chain
        , sum(raw_amount_precise::number / 1e18) as amt_pendle
        , SUM(raw_amount_precise::number / 1e18 * p.price) as amt_usd
    from
        arbitrum_flipside.core.ez_token_transfers
        left join arbitrum_flipside.price.ez_prices_hourly p ON p.hour = date_trunc('hour', block_timestamp)
        AND p.token_address = lower('0x0c880f6761F1af8d9Aa9C466984b80DAb9a8c9e8')
    where
        from_address = lower('0x1e56299ebc8a1010cec26005d12e3e5c5cc2db00') and -- GaugeController
        contract_address = lower('0x0c880f6761F1af8d9Aa9C466984b80DAb9a8c9e8')
    GROUP BY 1
)
SELECT
    date
    , chain
    , 'PENDLE' as token
    , sum(amt_pendle) as amt_pendle
    , sum(amt_usd) as amt_usd
FROM agg
GROUP BY 1, 2
ORDER BY 1 DESC