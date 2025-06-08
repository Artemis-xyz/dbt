{{ config(
    materialized="incremental",
    snowflake_warehouse="SOLANA_XLG",
    unique_key="date"
) }}

with stake_auction_marketplace_bid_flows as (
    select
        date(block_timestamp) as date
        , tx_id
        , signers[0]::string as signers
        -- bids flow as revenue
        , (decoded_args:claimSettlementArgs:claim / 1e9) as fees_native
        , (decoded_args:claimSettlementArgs:stakeAccountStaker::string) as stakeAccountStaker
        , (decoded_args:claimSettlementArgs:stakeAccountWithdrawer::string) as stakeAccountWithdrawer
    from solana_flipside.core.ez_events_decoded
    where 1=1
        and succeeded = true
        and program_id = 'vBoNdEvzMrSai7is21XgVYik65mqtaKXuSdMBJ1xkW4'
        and event_type = 'claimSettlementV2'
        and (decoded_args:claimSettlementArgs:stakeAccountStaker::string = '89SrbjbuNyqSqAALKBsKBqMSh463eLvzS4iVWCeArBgB'
            or decoded_args:claimSettlementArgs:stakeAccountWithdrawer::string = '89SrbjbuNyqSqAALKBsKBqMSh463eLvzS4iVWCeArBgB'
        )
)
, date_spine as (
    select
        ds.date
    from {{ ref("dim_date_spine") }} ds
    where date between (select min(date) from stake_auction_marketplace_bid_flows) and to_date(sysdate())
)
select
    ds.date
    , sum(coalesce(fees_native, 0)) as fees_native
from date_spine ds
left join stake_auction_marketplace_bid_flows using (date)
group by ds.date