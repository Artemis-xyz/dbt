{{
    config(
        materialized="incremental",
        unique_key= ['tx_hash', 'event_index'],
        snowflake_warehouse="MAPLE",
    )
}}

-- 521 rows as expected
select
    block_timestamp
    , tx_hash
    , event_index
    , block_number as block
    , contract_address
    , decoded_log:accountedInterest_::number as accountedInterest_
    , decoded_log:issuanceRate_::float as issuanceRate_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'AccountingStateUpdated'
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}