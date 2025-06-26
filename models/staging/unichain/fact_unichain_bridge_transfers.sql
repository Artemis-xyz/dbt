{{
    config(
        materialized="table",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}

{{ get_decoded_l1_superchain_bridge_events('unichain', '0x81014F44b0a345033bB2b3B21C7a1A308B35fEeA', 'artemis') }}
