{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_polygon_ecosystem_incentives",
    )
}}

{{ aave_v2_ecosystem_incentives('polygon', '0x357D51124f59836DeD84c8a1730D72B749d8BC23', 'AAVE V2')}}