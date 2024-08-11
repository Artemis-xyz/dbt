{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='view'
    )
}}


select
    *
from {{ref("dim_contracts_gold")}}