{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSISPAY",
        database="gnosispay",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

select
    date::date as date,
    token,
    sum(transfer_volume) as transfer_volume
from {{ ref('fact_gnosispay_transfers') }}
group by 1, 2