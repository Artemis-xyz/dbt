{{
    config(
        materialized="incremental",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_bsc_vrf_request_fulfilled_logs",
    )
}}
{{ chainlink_vrf_request_fulfilled_logs('bsc') }}