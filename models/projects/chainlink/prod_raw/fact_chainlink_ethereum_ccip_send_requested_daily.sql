{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ccip_send_requested_daily",
    )
}}


select
    'ethereum' as chain
    , evt_block_time::date as date_start
    , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
    , sum(ccip_send_requested.fee_token_amount) as fee_amount
    , ccip_send_requested.token as token
    , ccip_send_requested.destination_chain as destination_chain
    , count(ccip_send_requested.destination_chain) as count
from {{ref('fact_chainlink_ethereum_ccip_send_requested')}} ccip_send_requested
group by 2, 5, 6
order by 2, 5, 6