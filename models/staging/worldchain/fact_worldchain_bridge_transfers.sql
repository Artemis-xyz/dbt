{{
    config(
        materialized="table",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('worldchain', '0x470458C91978D2d929704489Ad730DC3E3001113', 'artemis') }}
