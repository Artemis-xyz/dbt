{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE"
    )
}}


with eth_incentives as (
    with eth_pendle_distribution_addresses as (
        SELECT
            distinct to_address as pendle_distributor_address
        FROM
            ethereum_flipside.core.ez_token_transfers
        WHERE 
            FROM_ADDRESS = lower('0X8119EC16F0573B7DAC7C0CB94EB504FB32456EE1')
            AND contract_address = lower('0x808507121B80c02388fAd14726482e061B8da827')
            AND to_address not in(
                lower('0X7BD456937104CA5EFFFBD895CCBBA52421021C29') -- EOA in charge of bridging
                , lower('0XF517364727FCC764D58DDF4E53280874A4D0C476') -- EOA in charge of bridging (2)
                , lower('0X99F5A734F746BD18BA3E1C4713008A98B7B1C067') -- Sends Pendle to Binance
                , lower('0XCEB82A7554C461B1DCB72B159CB649FE0A7D2A4C') -- Vesting contract?
            )
    )
    SELECT
        block_timestamp::date as date,
        'ethereum' as chain,
        sum(amount) as amt_pendle,
        sum(amount_usd) as amt_usd,
    FROM
        ethereum_flipside.core.ez_token_transfers
    WHERE 
        FROM_ADDRESS in (SELECT pendle_distributor_address FROM eth_pendle_distribution_addresses)
        AND contract_address = lower('0x808507121B80c02388fAd14726482e061B8da827')
    GROUP BY 1
)

, agg as(
    select
       date,
       chain,
       amt_pendle,
       amt_usd
    from
        eth_incentives
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