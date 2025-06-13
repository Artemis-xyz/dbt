{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="raw",
        alias="fact_eigenlayer_avs_rewards_submitted",
    )
}}


With AVSRewardsSubmittedEvents AS (
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_name,
        decoded_log,
        decoded_log:rewardsSubmission[1]::STRING as token_address,
        decoded_log:rewardsSubmission[2]::STRING as amount,
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    where contract_address = lower('0x7750d328b314EfFa365A0402CcfD489B80B0adda') --Eigenlayer Rewards Coordinator
    and event_name in ('RewardsSubmissionForAllEarnersCreated', 'AVSRewardsSubmissionCreated')
), token_info AS (
    SELECT
        hour,
        token_address,
        price,
        symbol,
        decimals
    FROM {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }}
), AVSRewardsSubmittedEvents_USD AS (
    select
        rse.*,
        t.decimals,
        rse.amount / pow(10,t.decimals) as amount_aduj,
        t.symbol as token_symbol,
        t.price as token_price,
        (rse.amount / POW(10, t.decimals)) * t.price AS amount_usd
    from AVSRewardsSubmittedEvents rse
    LEFT JOIN token_info t 
        ON lower(t.token_address) = lower(rse.token_address) 
        and t.hour = rse.date
) select * from AVSRewardsSubmittedEvents_USD