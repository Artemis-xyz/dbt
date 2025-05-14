{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('soneium', '0xeb9bf100225c214Efc3E7C651ebbaDcF85177607', 'artemis') }}
