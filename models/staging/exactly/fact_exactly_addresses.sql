{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY"
    )
}}

select
distinct '0x' || lower(substr(topic_1, 27, 40)) as address
from optimism_flipside.core.ez_decoded_event_logs l
where l.block_timestamp >= try_cast('2024-08-29' as timestamp)
and lower(topic_0) = lower('0x0b6a8f0ea14435788bae11ec53c2c0f6964bd797ab9a7f1c89773b87127131ba')