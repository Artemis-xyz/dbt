{{
    config(
        materialized="incremental",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_base_ccip_send_requested_logs_v1",
    )
}}

{{ chainlink_logs('base', ('0xaffc45517195d6499808c643bd4a7b0ffeedf95bea5852840d7bfcf63f59e821'))}}