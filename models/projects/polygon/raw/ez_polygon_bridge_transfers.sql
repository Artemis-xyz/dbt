{{
    config(
        materialized="view",
        snowflake_warehouse="POLYGON",
        database="polygon",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_polygon_pos_bridge_transfers')}}