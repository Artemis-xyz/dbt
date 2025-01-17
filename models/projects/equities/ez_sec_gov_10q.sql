{{
    config(
        materialized="table",
        snowflake_warehouse="X_SMALL",
        database="equities",
        schema="core",
        alias="ez_sec_metrics",
    )
}}

select * from {{ ref("fact_sec_gov_10q_pivot") }}
