{{
    config(
        materialized="table",
        snowflake_warehouse="CYPHER",
        database="CYPHER",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

select
    date::date as date,
    token,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_cypher_transfers") }}
group by 1, 2