{{config(materialized="table") }}

{{ distinct_eoa_addresses("tron") }}