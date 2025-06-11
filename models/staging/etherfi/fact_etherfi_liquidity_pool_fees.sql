{{ config(materialized="table", snowflake_warehouse="PUMPFUN") }}

with WithdrawRequest_and_NFTUnwrapped as (
    select
        e.*,
        e.decoded_log:fee::NUMBER as fee,
        fee / POW(10, t.decimals) as fee_adjusted,
        fee / POW(10, t.decimals) * t.price as fee_usd
    from {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}} e
    left join {{source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly')}} t
        on date_trunc('hour', e.block_timestamp) = t.hour
        and t.symbol = 'ETH' and t.name = 'ethereum'
    where
      topic_0 = '0x4ed779dfda2dd4cb90349b61fba6c125f68e3246023e6109203ddfa8db61ce05' -- WithdrawRequestClaimed event in WithdrawNFT contract
        or topic_0 = '0x411474e787b53f14ba5c38f00a6d38c5cb58edd2e074070aaef01ae9a34af9a7' -- NFTUnwrappedForEEth event in MembershipManager contract
) 
select
    date_trunc('day', block_timestamp) as date,
    sum(fee_adjusted) as fees_native,
    sum(fee_usd) as fees_usd
from WithdrawRequest_and_NFTUnwrapped 
where fee <> 0
group by 1