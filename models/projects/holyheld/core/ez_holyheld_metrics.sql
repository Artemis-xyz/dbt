{{
    config(
        materialized="table",
        snowflake_warehouse="HOLYHELD",
        database="holyheld",
        schema="core",
        alias="ez_metrics",
    )
}}

select 
    date::date as date,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_holyheld_transfers") }}
group by 1