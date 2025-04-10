{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("avalanche", "_v2") }}