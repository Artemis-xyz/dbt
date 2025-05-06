{{
    config(
        materialized="table",
        database="common",
        schema="core",
        snowflake_warehouse="COMMON",
    )
}}

select 
    *
from {{ ref("fact_protocol_datahub_gold") }}
