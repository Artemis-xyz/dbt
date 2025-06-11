{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("ethereum", "_v2") }}