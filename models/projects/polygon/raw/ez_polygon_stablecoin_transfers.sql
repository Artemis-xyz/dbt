{{
    config(
        materialized="view",
        snowflake_warehouse="POLYGON",
        database="polygon",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_polygon_stablecoin_transfers")}}