{{ config(snowflake_warehouse=var('snowflake_warehouse', default='EULER'), materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

{{ euler_ProxyCreated("ethereum", "0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e") }}