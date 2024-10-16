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
from {{source("GEO", "dim_geo_labels")}}