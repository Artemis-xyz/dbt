{{ config(materialized="table", unique_key="topic_zero", sort="topic_zero") }}
select
    value:"name"::string as event_name,
    value as event_info,
    {{ target.schema }}.event_info_to_keccak_event_signature(event_info) as topic_zero
from {{ ref("dim_contract_abis") }}, lateral flatten(input => abi) as f
where value:"type" = 'event'
