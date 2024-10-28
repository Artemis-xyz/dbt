{{
    config(
        materialized="incremental",
        snowflake_warehouse="MAPLE",
    )
}}

select
    block_timestamp,
    tx_hash,
    block_number as block,
    contract_address,
    decoded_log:accountedInterest_::number as accountedInterest_,
    decoded_log:domainEnd_::number as domainEnd_,
    decoded_log:issuanceRate_::float as issuanceRate_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'IssuanceParamsUpdated'
    and decoded_log:domainEnd_ is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}