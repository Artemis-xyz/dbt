{{config(materialized="incremental", snowflake_warehouse='MEDIUM')}}

with v1_arbi_logs as (
    select
        block_timestamp,
        tx_hash,
        decoded_log:reward::number/1e18 as reward_amount,
        lower(
            case 
                when decoded_log:rewardsToken::string is not null then decoded_log:rewardsToken::string
                else decoded_log:rewardToken::string
            end
        ) as from_address
    from
        arbitrum_flipside.core.ez_decoded_event_logs
    where
        contract_address = lower('0xc2054a8c33bfce28de8af4af548c48915c455c13')
        and event_name = 'RewardPaid'
    {% if is_incremental() %}
        and block_timestamp >= dateadd(day, -3, to_date(sysdate()))
    {% endif %}
)

, price_data as (
    {{ get_coingecko_metrics("radiant-capital") }}
)

, agg_logs as (
    select
        date(v.block_timestamp) as log_date,
        sum(reward_amount) as amount_native,
        sum(reward_amount * p.price) as amount_usd
    from v1_arbi_logs v
    left join price_data p
        on date(v.block_timestamp) = p.date
    group by date(v.block_timestamp)
)

, arbi_v1_data as (
    select
        log_date as date
        , 'arbitrumV1' as chain
        , sum(amount_native) as amount_native
        , sum(amount_usd) as amount_usd
    from agg_logs
    group by 1, 2
)

, arbi_v2_data as (
    select
        cast(block_timestamp as date) as date
        , 'arbitrumV2' as chain
        , sum(amount) as amount_native
        , sum(amount_usd) as amount_usd
    from arbitrum_flipside.core.ez_token_transfers
    where contract_address = lower('0x3082CC23568eA640225c2467653dB90e9250AaA0')
      and from_address in (lower('0xebc85d44cefb1293707b11f707bd3cec34b4d5fa'))
      -- moving RDNT from the incentives contract to a multisig wallet (not token incentives)
      and tx_hash != '0xefff541c1bf4208cc8f1c5961297fa45c9ba235992cc5f87820b68f5e8614f30'
    group by 1, 2
)

, arbi_data as (
    select
        date
        , chain
        , amount_native
        , amount_usd
    from arbi_v1_data

    union all

    select
        date
        , chain
        , amount_native
        , amount_usd
    from arbi_v2_data
)

select
    date
    , chain
    , amount_native
    , amount_usd
from arbi_data
order by date, chain