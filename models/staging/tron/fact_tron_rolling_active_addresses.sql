{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("tron", "_v2") }}