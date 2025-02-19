{{ config(materialized="table", unique_key="topic_zero", sort="topic_zero") }}
with
event_signatures as (
    select
        value:"name"::string as event_name,
        value as event_info,
        {{ target.schema }}.event_info_to_keccak_event_signature_v2(value) as topic_zero,
        row_number() over (partition by topic_zero order by event_name) as event_id
    from {{ ref("dim_contract_abis") }}, lateral flatten(input => abi) as f
    where value:"type" = 'event'
)
select 
    event_name,
    event_info,
    topic_zero
from event_signatures
where event_id = 1 and event_name not in ('AuthorizationCanceled', 'ValidatorEcdsaPublicKeyUpdated', 'ValidatorBlsPublicKeyUpdated', 'AuthorizationUsed', 'TransferComment') -- These events emit types currently not supported by the decode_evm_event_log function
