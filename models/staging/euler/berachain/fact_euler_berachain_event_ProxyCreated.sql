{{ config(snowflake_warehouse=var('snowflake_warehouse', default='EULER'), materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ euler_ProxyCreated("berachain", "0x5C13fb43ae9BAe8470f646ea647784534E9543AF") }}