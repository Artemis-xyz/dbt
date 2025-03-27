{{
    config(
        materialized="table",
        snowflake_warehouse="CYPHER",
        database="CYPHER",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date::date as date,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_cypher_transfers") }}
group by 1