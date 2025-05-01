{{ config(snowflake_warehouse="EULER", materialized="incremental", unique_key=["transaction_hash", "event_index"], enabled=false) }}

{{ euler_VaultStatus("berachain", "0x5C13fb43ae9BAe8470f646ea647784534E9543AF") }}