{{
    config(
        materialized="incremental",
        snowflake_warehouse="MAPLE",
    )
}}

select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:account_::string as account_
    , decoded_log:assetsToWithdraw_::number as assetsToWithdraw_
    , decoded_log:sharesToRedeem_::number as sharesToRedeem_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'WithdrawalProcessed'
    and decoded_log:assetsToWithdraw_ is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %} 