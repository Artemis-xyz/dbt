{{ 
    config(
        materialized="table",
        unique_key=["topic_zero", "indexed_topic_count"],
        snowflake_warehouse='BALANCES_LG',
    ) 
}}
with
event_signatures as (
    select
        value:"name"::string as event_name,
        value as event_info,
        {{ target.schema }}.event_info_to_keccak_event_signature_v3(value) as topic_zero,
        
        'artemis' as source
    from {{ ref("dim_contract_abis") }}, lateral flatten(input => abi) as f
    where value:"type" = 'event'

    UNION ALL 

    select
        value:"name"::string as event_name,
        value as event_info,
        {{ target.schema }}.event_info_to_keccak_event_signature_v3(value) as topic_zero,
        source
    from {{ source("DECODING", "dim_all_abis") }}, lateral flatten(input => abi) as f
    where value:"type" = 'event'
)
, event_signatures_with_row_id as (
    select
        event_name,
        event_info,
        topic_zero,
        source,
        row_number() over (partition by topic_zero order by event_name) as row_id
    from event_signatures
)
, event_signatures_with_index_number as (
    select
        event_name,
        event_info,
        topic_zero,
        source,
        row_id,
        COUNT_IF(f.value:"indexed"::BOOLEAN = TRUE) AS indexed_topic_count
    from event_signatures_with_row_id, LATERAL FLATTEN(input => event_info:"inputs") AS f
    group by event_name, event_info, topic_zero, source, row_id
)
select 
    event_name,
    event_info,
    topic_zero,
    indexed_topic_count,
    source
from event_signatures_with_index_number
QUALIFY ROW_NUMBER() OVER (PARTITION BY topic_zero, indexed_topic_count ORDER BY source = 'artemis' DESC) = 1
