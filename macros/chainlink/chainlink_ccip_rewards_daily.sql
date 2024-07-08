{% macro chainlink_ccip_rewards_daily(chain) %}
with

    combined_logs as ( 
        select
            ccip_logs_v1.chain
            , ccip_logs_v1.block_timestamp
            , token_addresses.token_symbol AS token
            , decoded_log:"message":"feeTokenAmount"::number / 1e18 as fee_token_amount
            , decoded_log:"message":"feeToken"::string as fee_token
            , onramp_meta.chain_selector as destination_selector
            , onramp_meta.chain as destination_chain
            , ccip_logs_v1.tx_hash
        from {{ ref('fact_chainlink_' ~ chain ~ '_ccip_send_requested_logs_v1') }} ccip_logs_v1
        left join {{ref('dim_chainlink_' ~ chain ~'_ccip_onramp_meta')}} onramp_meta on lower(onramp_meta.onramp) = lower(contract_address)
        left join {{ ref('dim_chainlink_' ~ chain ~'_ccip_token_meta') }} token_addresses on lower(token_addresses.token_contract) = lower(ccip_logs_v1.decoded_log:"message":"feeToken"::string)

        union all

        select
            ccip_logs_v1_2.chain
            , ccip_logs_v1_2.block_timestamp
            , token_addresses.token_symbol AS token
            , decoded_log:"message":"feeTokenAmount"::number / 1e18 as fee_token_amount
            , decoded_log:"message":"feeToken"::string as fee_token
            , onramp_meta.chain_selector as destination_selector
            , onramp_meta.chain as destination_chain
            , ccip_logs_v1_2.tx_hash
        from {{ ref('fact_chainlink_' ~ chain ~ '_ccip_send_requested_logs_v1_2') }} ccip_logs_v1_2
        left join {{ref('dim_chainlink_' ~ chain ~ '_ccip_onramp_meta')}} onramp_meta on lower(onramp_meta.onramp) = lower(contract_address)
        left join {{ ref('dim_chainlink_' ~ chain ~ '_ccip_token_meta') }} token_addresses on lower(token_addresses.token_contract) = lower(ccip_logs_v1_2.decoded_log:"message":"feeToken"::string)
    )
    , ccip_send_requested as (
        select
            max(chain) AS chain
            , max(block_timestamp) AS evt_block_time
            , sum(fee_token_amount) AS fee_token_amount
            , max(token) AS token
            , max(fee_token) AS fee_token
            , max(destination_selector) AS destination_selector
            , max(destination_chain) AS destination_chain
            , max(tx_hash) AS tx_hash
        from combined_logs
        group by tx_hash
    )
    , ccip_send_requested_daily as (
        select
            evt_block_time::date as date_start
            , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
            , sum(ccip_send_requested.fee_token_amount) as fee_amount
            , ccip_send_requested.token as token
            , ccip_send_requested.destination_chain as destination_chain
            , count(ccip_send_requested.destination_chain) as count
        from ccip_send_requested
        group by date_start, token, destination_chain
    )
    , eth_price as ({{ get_coingecko_price_with_latest('ethereum') }})
    , link_price as ({{ get_coingecko_price_with_latest('chainlink') }})
    , token_usd_daily AS (
        select 
            date as date_start
            , 'WETH' as symbol
            , price as usd_amount
        from eth_price

        union all

        select 
            date as date_start
            , 'LINK' as symbol
            , price as usd_amount
        from link_price
    )
    , ccip_reward_daily AS (
        select
            ccip_send_requested_daily.date_start
            , cast(date_trunc('month', ccip_send_requested_daily.date_start) as date) as date_month
            , sum(ccip_send_requested_daily.fee_amount) as token_amount
            , sum((ccip_send_requested_daily.fee_amount * tud.usd_amount)) as usd_amount
            , ccip_send_requested_daily.token as token
        from ccip_send_requested_daily ccip_send_requested_daily
        left join token_usd_daily tud ON tud.date_start = ccip_send_requested_daily.date_start AND tud.symbol = ccip_send_requested_daily.token
        group by 1, 5
    )

select
    '{{ chain }}' as chain,
    date_start,
    date_month,
    token_amount,
    usd_amount,
    token
from ccip_reward_daily
order by 2, 6

{% endmacro %}