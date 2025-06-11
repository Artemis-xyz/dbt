{{ config(snowflake_warehouse=var('snowflake_warehouse', default='EULER'), materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ euler_VaultStatus("base", "0x7F321498A801A191a93C840750ed637149dDf8D0") }}