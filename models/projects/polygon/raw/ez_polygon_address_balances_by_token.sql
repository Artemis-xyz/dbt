{{
    config(
        materialized="view",
        snowflake_warehouse="POLYGON",
        database="polygon",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_polygon_address_balances_by_token") }}