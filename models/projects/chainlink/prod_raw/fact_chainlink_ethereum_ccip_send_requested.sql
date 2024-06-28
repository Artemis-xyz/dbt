{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ccip_send_requested",
    )
}}

with 
combined_logs as ( 
    select
        ccip_logs_v1.chain
        , ccip_logs_v1.block_timestamp
        , ccip_logs_v1.fee_token_amount / 1e18 AS fee_token_amount
        , token_addresses.token_symbol AS token
        , ccip_logs_v1.fee_token
        , ccip_logs_v1.destination_selector
        , ccip_logs_v1.destination_chain
        , ccip_logs_v1.tx_hash
    from {{ ref('fact_chainlink_ethereum_ccip_send_requested_logs_v1') }} ccip_logs_v1
    left join {{ ref('dim_chainlink_ethereum_ccip_token_meta') }} token_addresses ON lower(token_addresses.token_contract) = lower(ccip_logs_v1.fee_token)

    union all

    select
        ccip_logs_v1_2.chain
        , ccip_logs_v1_2.block_timestamp
        , ccip_logs_v1_2.fee_token_amount / 1e18 AS fee_token_amount
        , token_addresses.token_symbol AS token
        , ccip_logs_v1_2.fee_token
        , ccip_logs_v1_2.destination_selector
        , ccip_logs_v1_2.destination_chain
        , ccip_logs_v1_2.tx_hash
    from {{ ref('fact_chainlink_ethereum_ccip_send_requested_logs_v1_2') }} ccip_logs_v1_2
    left join {{ ref('dim_chainlink_ethereum_ccip_token_meta') }} token_addresses ON lower(token_addresses.token_contract) = lower(ccip_logs_v1_2.fee_token)
)

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