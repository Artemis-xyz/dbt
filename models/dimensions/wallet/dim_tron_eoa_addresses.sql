{{config(materialized="incremental", unique_key="address") }}

{{ distinct_eoa_addresses("tron") }}