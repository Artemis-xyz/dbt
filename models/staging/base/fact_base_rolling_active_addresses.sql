{{
    config(
        materialized="incremental",
        unique_key=["date"],
    )
}}

{{ rolling_active_addresses("base", "_v2") }}